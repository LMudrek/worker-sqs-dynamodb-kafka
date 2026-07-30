"""
Módulo de Ingestão Assíncrona de Parquet para Data Lake.

Este módulo recebe um DataFrame Pandas, calcula o tamanho seguro por chunk 
(em MB) usando amostragem, converte os dados para Parquet em memória (BytesIO),
e faz o envio HTTP para a plataforma de dados via API Gateway com suporte 
automático a retries em caso de falhas transitórias.
"""

import io
import math
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configuração de observabilidade
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataLakeUploader:
    """Responsável exclusivo pela comunicação HTTP e resiliência de rede."""
    
    def __init__(self, api_url: str, auth_token: str):
        self.api_url = api_url
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {auth_token}"})

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True
    )
    def send_chunk(self, parquet_bytes: bytes, metadata: dict) -> requests.Response:
        """Envia um chunk binário via multipart/form-data com retry exponencial."""
        files = {
            'file': ('chunk.parquet', parquet_bytes, 'application/octet-stream')
        }
        data = {
            'metadata': pd.io.json.dumps(metadata)
        }
        
        size_mb = len(parquet_bytes) / (1024 * 1024)
        logger.info(
            f"[Interaction: {metadata.get('interactionId')}] "
            f"Enviando chunk {metadata.get('sequence')}/{metadata.get('total_chunks')} "
            f"({size_mb:.2f} MB)..."
        )
        
        response = self.session.post(self.api_url, files=files, data=data, timeout=15)
        response.raise_for_status()
        return response


class ParquetChunkerManager:
    """Responsável por orquestrar a divisão do DataFrame e o processamento em background."""
    
    def __init__(self, uploader: DataLakeUploader, max_workers: int = 3):
        self.uploader = uploader
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def _estimate_safe_row_chunk_size(self, df: pd.DataFrame, interaction_id: str, target_mb: float = 5.0) -> int:
        """Calcula quantas linhas cabem com segurança no payload alvo (target_mb) usando amostragem."""
        target_bytes = target_mb * 1024 * 1024
        sample_size = min(1000, len(df))
        
        # 1. Isola a amostra
        sample_df = df.head(sample_size).copy()
        
        # 2. Injeta as colunas auxiliares para simular o tamanho final real
        sample_df['interaction_id'] = interaction_id
        sample_df['sequence'] = 99999
        sample_df['total_chunks'] = 99999
        
        # 3. Serializa em memória para descobrir a taxa de compressão
        buf = io.BytesIO()
        table = pa.Table.from_pandas(sample_df)
        pq.write_table(table, buf, compression='snappy')
        sample_bytes = len(buf.getvalue())
        
        # 4. Calcula bytes por linha na amostra
        bytes_per_row = sample_bytes / sample_size
        
        # Fator de segurança de 20% (0.8) para absorver variância nos dados da população total
        safe_rows = int((target_bytes / bytes_per_row) * 0.8)
        
        return max(1, safe_rows)

    def process_and_send_async(self, df: pd.DataFrame, dataset_name: str, interaction_id: str, target_mb: float = 5.0):
        """
        Método não-bloqueante (Fire-and-Forget) para despachar a ingestão.
        O interaction_id agora é fornecido pela aplicação consumidora.
        """
        self.executor.submit(
            self._chunk_and_upload_task, df, dataset_name, interaction_id, target_mb
        )
        logger.info(f"Ingestão do interaction_id '{interaction_id}' enviada para background.")

    def _chunk_and_upload_task(self, df: pd.DataFrame, dataset_name: str, interaction_id: str, target_mb: float):
        """Task principal que roda na thread de background."""
        try:
            total_rows = len(df)
            
            # Calcula dinamicamente o número seguro de linhas para bater o target em MB
            chunk_size = self._estimate_safe_row_chunk_size(df, interaction_id, target_mb)
            total_chunks = math.ceil(total_rows / chunk_size)
            
            logger.info(
                f"[Task {interaction_id}] Ingestão estruturada: "
                f"Alvo {target_mb}MB -> Chunks de ~{chunk_size} linhas. Total de chunks: {total_chunks}."
            )

            for i in range(total_chunks):
                sequence = i + 1
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, total_rows)
                
                # Fatiamento em memória
                df_chunk = df.iloc[start_idx:end_idx].copy()
                
                # Injeção das colunas de consistência
                df_chunk['interaction_id'] = interaction_id
                df_chunk['sequence'] = sequence
                df_chunk['total_chunks'] = total_chunks
                
                # Conversão para Parquet (Buffer de memória)
                parquet_buffer = io.BytesIO()
                table = pa.Table.from_pandas(df_chunk)
                pq.write_table(table, parquet_buffer, compression='snappy')
                parquet_bytes = parquet_buffer.getvalue()
                
                # Metadados obrigatórios do contrato da API
                metadata_api = {
                    "dataset": dataset_name,
                    "interactionId": interaction_id,
                    "sequence": sequence,
                    "total_chunks": total_chunks
                }
                
                # Envio HTTP com retry encapsulado
                self.uploader.send_chunk(parquet_bytes, metadata_api)
                
            logger.info(f"[Task {interaction_id}] Ingestão finalizada com sucesso!")
            
        except Exception as e:
            logger.error(f"[Task {interaction_id}] Falha catastrófica no processamento: {str(e)}", exc_info=True)


# ==========================================
# Exemplo de Uso na Aplicação
# ==========================================
if __name__ == "__main__":
    # 1. Configuração estática no startup da aplicação
    uploader = DataLakeUploader(
        api_url="https://api.seudominio.com/v1/datasets/files",
        auth_token="token-jwt-ou-api-key"
    )
    manager = ParquetChunkerManager(uploader=uploader, max_workers=5)
    
    # 2. Simulação de um DataFrame de negócio
    dados_exemplo = {
        "cliente_id": range(1, 200000),
        "valor": [x * 1.5 for x in range(1, 200000)],
        "status": ["ativo"] * 199999
    }
    df_negocio = pd.DataFrame(dados_exemplo)
    
    # 3. O ID de interação gerado pelo seu domínio/negócio 
    id_rastreio_negocio = "TRD-2026-07-30-XYZ987"
    
    # 4. Disparo assíncrono (não trava a main thread)
    manager.process_and_send_async(
        df=df_negocio, 
        dataset_name="operacoes_trade", 
        interaction_id=id_rastreio_negocio, 
        target_mb=5.0
    )
    
    logger.info("O processamento de negócio segue a vida livremente aqui...")
