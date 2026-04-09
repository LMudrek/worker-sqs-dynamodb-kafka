Crie uma história de usuário completa, clara e pronta para desenvolvimento, considerando o contexto abaixo.

A história deve ser escrita com foco no valor de negócio (usuário final), evitando linguagem excessivamente técnica no topo, mas incluindo detalhamento técnico nas seções apropriadas.

---

## Contexto

Atualmente, a API de consulta utiliza um modelo de dados no DynamoDB onde:
- Cada macrocategoria é armazenada em um único item
- As categorias estão encapsuladas dentro de um JSON

Esse modelo apresenta limitações de:
- Escalabilidade (crescimento do payload)
- Performance (leitura de dados desnecessários)
- Baixa granularidade para evolução e manutenção

Deseja-se evoluir o modelo para uma abordagem granular, onde:
- Cada combinação de (macro + categoria) é armazenada como um item independente
- A chave segue o padrão:

PK = <id>
SK = MOTOR#<macro>#<categoria>

A API deve continuar atendendo os mesmos cenários funcionais:
1. Consulta por múltiplas macrocategorias
2. Consulta por macrocategoria + categoria
3. Consulta sem filtro (respeitando allowlist de macrocategorias)

---

## Objetivo da história

Evoluir a API de consulta para utilizar o novo modelo granular no DynamoDB, garantindo:
- Melhor eficiência de leitura
- Maior controle sobre os dados
- Escalabilidade futura
- Compatibilidade com regras atuais de negócio

---

## Instruções para geração da história

A história deve conter obrigatoriamente:

### 1. Título claro e objetivo

### 2. Descrição no formato:
EU COMO <tipo de usuário ou sistema consumidor>  
DESEJO <ação>  
PARA <benefício de negócio>

---

### 3. Dor atendida

Descrever problemas atuais do modelo encapsulado, como:
- Leitura excessiva de dados
- Dificuldade de evolução
- Limitações de escala

---

### 4. Escopo da entrega

Descrever claramente o que está incluído:
- Alteração da estratégia de leitura no DynamoDB
- Uso de queries com begins_with
- Implementação de fan-out para múltiplas macros
- Manutenção da interface da API (sem breaking change, se aplicável)

---

### 5. Regras de negócio

- Apenas macrocategorias permitidas devem ser retornadas
- Combinação (macro + categoria) é única
- A API deve suportar os três cenários de consulta
- Não deve haver mudança no contrato externo (se aplicável)

---

### 6. Detalhes técnicos

- Uso de PK = id
- Uso de SK = MOTOR#<macro>#<categoria>
- Queries utilizando KeyConditionExpression
- Uso de begins_with para macro
- Fan-out controlado para múltiplas macros
- Proibição de Scan
- Proibição de FilterExpression para simular IN
- Tratamento de paginação
- Controle de paralelismo

---

### 7. Requisitos de aceite

Criar critérios objetivos, incluindo:

- Dado um id e múltiplas macros, quando consultar, então deve retornar apenas os dados dessas macros
- Dado um id, macro e categoria, quando consultar, então deve retornar apenas um item
- Dado um id sem filtro, quando consultar, então deve respeitar a allowlist
- A consulta não deve utilizar Scan
- A latência deve ser consistente mesmo com múltiplas macros
- O resultado deve ser funcionalmente equivalente ao modelo anterior

---

### 8. Critérios não funcionais

- Performance previsível
- Escalabilidade para crescimento de categorias
- Baixo acoplamento
- Observabilidade (logs/monitoramento de fan-out)

---

Importante:
- Linguagem clara, direta e sem redundância
- Estrutura organizada e legível
- Foco em valor de negócio antes da técnicau