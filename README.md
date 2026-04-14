# Desafio técnico — Airflow, Python e PySpark
## Visão geral

Este projeto implementa um pipeline local orquestrado com Apache Airflow para processar dados transacionais da tabela _orders_raw__s, aplicando janela temporal, tratamento de duplicidade, captura de registros tardios e geração de uma visão analítica consolidada.

## Estratégia temporal

A cada execução, o pipeline processa a data de referência da DAG e também um número configurável de dias anteriores (_lookback_). Essa estratégia permite capturar eventos com atraso de ingestão sem necessidade de mecanismos mais complexos.

## Estratégia de deduplicação

A deduplicação acontece em duas etapas:

1. remoção de duplicação por **event_id**
2. consolidação do estado mais recente por **order_id** + **business_date**, com base em **ingested_at**

Isso permite tratar tanto reenvios quanto atualizações operacionais do mesmo pedido.

## Idempotência

O pipeline é idempotente para a janela processada. Antes de gravar o resultado consolidado, remove do destino os dados daquela janela e insere novamente a versão recalculada.

## Como executar localmente

```bash
git clone <repo>
cd technical-challenge-airflow-pyspark
docker compose up --build
```

Airflow disponível em:

```bash
http://localhost:8080
```

> obs: caso error ou falha de execução rodar o comando abaixo.
> ```docker compose up airflow-webserver```

### CRIAÇÃO DE USUÁRIO

Caso não consiga logar com user: `admini` e password `admin`
crie um usuário com o command:

```bash
docker compose exec airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@email.com --password admin
```

Depois:

* habilite a DAG orders_analytics_pipeline
* execute manualmente ou aguarde o scheduler

### Como validar a saída

* verificar a tabela orders_analytics no arquivo data/analytics.db
* verificar arquivos parquet em data/output/
* verificar logs da task no Airflow UI

### Trade-offs adotados
* SQLite foi usado para simplificar execução local
* leitura foi feita via pandas + conversão para Spark para manter a *implementação acessível
* em produção, o ideal seria leitura direta via JDBC ou lakehouse
* a idempotência foi implementada por substituição da janela, em vez de merge incremental sofisticado

### Evolução para produção

Em cloud, essa solução poderia evoluir para:

* orquestração com Airflow gerenciado
* armazenamento em object storage com Parquet/Delta
* processamento em Spark gerenciado
* controle de estado com tabelas particionadas
* observabilidade com logs centralizados, métricas e alertas
* uso de MERGE para upsert incremental
* política de custo com cluster efêmero e processamento por partição

### Simplificações desta implementação local
* sem secret manager
* sem fila/event-driven
* sem catálogo de metadados
* sem monitoramento externo
* sem gerenciamento avançado de schema evolution


## RESUMO

Escolhi uma arquitetura simples o suficiente para rodar localmente, mas que já antecipa preocupações reais de produção. O ponto principal foi tratar o tempo corretamente usando uma janela com lookback para capturar registros tardios, além de garantir idempotência e deduplicação. Separei a orquestração da lógica de transformação para deixar a DAG mais limpa e facilitar evolução futura. Também optei por uma regra explícita de consolidação: para cada pedido e data de negócio, vale o evento mais recente por ingested_at, o que resolve retries e updates operacionais.