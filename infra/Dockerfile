# ---------- BUILD ----------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /app

# copia csproj e restaura dependências
COPY ["SqsWorkerKafka.csproj", "./"]
RUN dotnet restore "./SqsWorkerKafka.csproj"

# copia o restante e publica
COPY . .
RUN dotnet publish "SqsWorkerKafka.csproj" -c Release -o /app/publish /p:UseAppHost=false

# ---------- RUNTIME ----------
FROM mcr.microsoft.com/dotnet/runtime:10 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

# variáveis default (override no compose)
ENV DOTNET_ENVIRONMENT=Production

ENTRYPOINT ["dotnet", "SqsWorkerKafka.dll"]