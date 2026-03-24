# ---------- BUILD ----------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# copia csproj e restaura dependencias
COPY ["app/SqsWorkerKafka/SqsWorkerKafka.csproj", "app/SqsWorkerKafka/"]
RUN dotnet restore "app/SqsWorkerKafka/SqsWorkerKafka.csproj"

# copia o restante e publica
COPY . .
WORKDIR /src/app/SqsWorkerKafka
RUN dotnet publish "SqsWorkerKafka.csproj" -c Release -o /app/publish /p:UseAppHost=false

# ---------- RUNTIME ----------
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

# variaveis default (override no compose)
ENV DOTNET_ENVIRONMENT=Production

ENTRYPOINT ["dotnet", "SqsWorkerKafka.dll"]
