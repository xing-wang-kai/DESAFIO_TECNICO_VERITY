from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"                       ## PASTA PARA SALVAR OS ARQUIVOS
OUTPUT_DIR = DATA_DIR / "output"                   ## PASTA DE SAÍDA DOS DADOS
SOURCE_DB = DATA_DIR / "source.db"                 ## CAMINHO PARA O BANCO DE DADOS DE ORIGEM
TARGET_DB = DATA_DIR / "analytics.db"              ## CAMINHO PARA O BANCO DE DADOS ANALITICOS

LOOKBACK_DAYS = 2                                  ## QUANTIDADE DE DIAS PARA RETORNO DOS DADOS EM BACKLOG
APP_NAME = "orders-analytics-job"                  ## NOME DO APPs