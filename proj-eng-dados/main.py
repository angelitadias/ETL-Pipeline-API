import requests
import json
import os                     
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import pyarrow.dataset as ds 
import pyarrow.parquet as pq
import pyarrow as pa


# --- Configuração ---
load_dotenv()

# A função os.getenv() lê a variável que o load_dotenv() carregou.
API_TOKEN = os.getenv("API_TOKEN") 

# o script para com uma mensagem de erro clara.
if not API_TOKEN:
    raise ValueError("API_TOKEN não encontrado! Verifique seu arquivo .env")

# variáveis para o dataset "gastos-diretos"
DATASET_SLUG = "gastos-diretos"
TABLE_NAME = "gastos"

API_URL = f"https://api.brasil.io/v1/dataset/{DATASET_SLUG}/{TABLE_NAME}/data/"

# Define os caminhos das pastas usando pathlib
BASE_DIR = Path(__file__).resolve().parent
RAW_PATH = BASE_DIR / "dataset" / "raw"
BRONZE_PATH = BASE_DIR / "dataset" / "bronze"
SILVER_PATH = BASE_DIR / "dataset" / "silver"
GOLD_PATH = BASE_DIR / "dataset" / "gold"

# Garante que os diretórios existam (exist_ok)
RAW_PATH.mkdir(parents=True, exist_ok=True)
BRONZE_PATH.mkdir(parents=True, exist_ok=True)
SILVER_PATH.mkdir(parents=True, exist_ok=True)  
GOLD_PATH.mkdir(parents=True, exist_ok=True)  


def fetch_and_save_raw_data():
    # Busca os dados da API, tratando a paginação, e as páginas como arquivos JSON na pasta 'raw'.

    print(f"Iniciando busca de dados da API: {DATASET_SLUG}/{TABLE_NAME}...")

    headers = {"Authorization": f"Token {API_TOKEN}"}  # Token de autenticação
    url = API_URL
    page_count = 1

    # Verifica páginas já baixadas
    paginas_existentes = {
        int(f.stem.split('_')[-1])
        for f in RAW_PATH.glob(f"{DATASET_SLUG}_{TABLE_NAME}_page_*.json")
    }

    while url:
        # pula páginas já baixadas
        if page_count in paginas_existentes:
            print(f"Página {page_count} já existente. Pulando...")
            page_count += 1
            url = f"{API_URL}?page={page_count}"
            continue

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                print("  Rate limit atingido. Aguardando 15 segundos...")
                import time
                time.sleep(15)
                continue

            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])

            if not results:
                print("Nenhum resultado encontrado nesta página. Encerrando coleta.")
                break

            # Salva JSON bruto
            filename = f"{DATASET_SLUG}_{TABLE_NAME}_page_{page_count}.json"
            filepath = RAW_PATH / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            print(f"Página {page_count} salva em {filepath}")

            # próxima página
            url = data.get("next")
            page_count += 1

            import time
            time.sleep(1)  # Delay entre as requisições

        except requests.exceptions.RequestException as e:
            print(f"Erro ao acessar a API: {e}")
            break

    print("Busca de dados brutos concluída.")


def process_raw_to_bronze():
    # Lê todos os arquivos JSON da pasta 'raw', transforma em Parquet e salva na pasta 'bronze', particionado por ano e mês.
    
    print("\nIniciando processamento para a camada Bronze...")

    json_files = sorted(RAW_PATH.glob(f"{DATASET_SLUG}_{TABLE_NAME}_page_*.json"))
    if not json_files:
        print("Nenhum arquivo JSON encontrado na pasta 'raw'.")
        return

    all_data = []
    for file in json_files:
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "results" in data:
                    all_data.extend(data["results"])
        except json.JSONDecodeError as e:
            print(f"Erro ao ler o arquivo {file}: {e}")
        except Exception as e:
            print(f"Erro inesperado ao processar {file}: {e}")

    if not all_data:
        print("Nenhum dado para processar.")
        return

    # Converte para DataFrame
    df = pd.DataFrame(all_data)
    print(f"Total de registros consolidados: {len(df)}")

    # Verifica colunas de partição
    partition_cols = ['ano', 'mes']
    if not all(col in df.columns for col in partition_cols):
        print(f"Erro: O DataFrame não contém as colunas de partição esperadas ({partition_cols}).")
        print(f"Colunas disponíveis: {df.columns.to_list()}")
        return

    # Converte tipos
    try:
        df['ano'] = df['ano'].astype(int)
        df['mes'] = df['mes'].astype(int)
    except Exception as e:
        print(f"Aviso: Não foi possível converter colunas 'ano' e 'mes' para inteiro. {e}")

    # Salva em Parquet particionado por ano/mês
    print(f"Salvando dados na camada Bronze ({BRONZE_PATH})...")

    try:
        df.to_parquet(
            BRONZE_PATH,
            engine='pyarrow',
            compression='snappy',
            partition_cols=partition_cols
        )
        print("Processamento para Bronze concluído com sucesso!")
        print(f"Dados salvos particionados em: {BRONZE_PATH}")

    except Exception as e:
        print(f"Erro ao salvar arquivo Parquet: {e}")
        if "PYARROW_FS_S3_EMPTY_PATH_NOT_ALLOWED" in str(e):
            print("Dica: Este erro pode ocorrer se o BRONZE_PATH estiver vazio ou incorreto.")


def run_data_quality_tests(df):
    """
    Executa um conjunto simples de testes de qualidade de dados (Data Quality).
    Gera um erro (AssertionError) se um teste falhar.
    """
    print("Executando testes de qualidade de dados (Data Quality Checks)...")

    # Teste 1: Colunas críticas não devem ter valores nulos
    critical_cols = ['ano', 'mes', 'nome_orgao', 'nome_favorecido']
    for col in critical_cols:
        assert col in df.columns, f"Coluna crítica '{col}' não encontrada no dataset."
        assert df[col].notna().all(), f"Coluna crítica '{col}' possui valores nulos."

    # Teste 2: Valores de mês devem estar entre 1 e 12
    assert df['mes'].between(1, 12).all(), "Valores inválidos encontrados na coluna 'mes'."

    # Teste 3: Valores monetários não devem ser negativos
    assert (df['valor'] >= 0).all(), "Valores negativos encontrados na coluna 'valor'."

    print(" Testes de qualidade aprovados!")


def process_bronze_to_silver():
    """
    Lê dados da camada Bronze (Parquet), aplica limpeza, padronização,
    testes de qualidade e salva na camada Silver.
    """
    print("\n--- CAMADA SILVER ---")
    print("Iniciando processamento para a camada Silver (Limpeza e Padronização)...")
    
    try:
        df = pd.read_parquet(BRONZE_PATH)
    except Exception as e:
        print(f"Erro ao ler dados da camada Bronze: {e}")
        return

    if df.empty:
        print("Nenhum dado encontrado na camada Bronze.")
        return

    # --- Limpeza e Padronização ---

    df_silver = df.copy()

    # REGRA 1: TRATAR VALORES NULOS
    if 'valor' in df_silver.columns:
        df_silver['valor'] = pd.to_numeric(df_silver['valor'], errors='coerce').fillna(0)

    # REGRA 2: PADRONIZAÇÃO DE TEXTO
    text_cols = [
        'nome_orgao',
        'nome_favorecido',
        'nome_acao',
        'nome_programa',
        'nome_funcao',
        'nome_grupo_despesa'
    ]
    for col in text_cols:
        if col in df_silver.columns:
            df_silver[col] = df_silver[col].astype(str).str.upper().str.strip()

    # REGRA 3: GARANTIA DE TIPOS NUMÉRICOS
    for num_col in ['ano', 'mes']:
        if num_col in df_silver.columns:
            df_silver[num_col] = pd.to_numeric(df_silver[num_col], errors='coerce').astype('Int64')

    # --- Testes de Qualidade ---
    try:
        run_data_quality_tests(df_silver)
    except AssertionError as e:
        print(f" Teste de Qualidade Falhou: {e}")
        print("Processamento Silver interrompido devido à baixa qualidade dos dados.")
        return

    # --- Análise Exploratória Simples ---
    print("\nAnálise Exploratória:")
    print("Total de linhas:", len(df_silver))
    print("Total de órgãos únicos:", df_silver['nome_orgao'].nunique() if 'nome_orgao' in df_silver else 0)
    print("Faixa de datas:", 
      df_silver['data_pagamento'].min() if 'data_pagamento' in df_silver else 'N/A', 
      "→", 
      df_silver['data_pagamento'].max() if 'data_pagamento' in df_silver else 'N/A')
    print("Valor médio de pagamento:", round(df_silver['valor'].mean(), 2))

    # Converte data_pagamento (melhorando o tipo de dado)
    if 'data_pagamento' in df_silver.columns:
        df_silver['data_pagamento'] = pd.to_datetime(df_silver['data_pagamento'], errors='coerce')


    # --- Gravação ---
    print(f"Salvando dados limpos na camada Silver ({SILVER_PATH})...")
    try:
        df_silver.to_parquet(
            SILVER_PATH,
            engine='pyarrow',
            compression='snappy',
            partition_cols=['ano', 'mes']
        )
        print(" Processamento para Silver concluído com sucesso!")
    except Exception as e:
        print(f"Erro ao salvar arquivo Parquet na camada Silver: {e}")


def process_silver_to_gold():
    """
    Lê dados da camada Silver (limpos), aplica agregações de negócio
    e salva na camada Gold (pronto para BI).
    """
    import pyarrow.dataset as ds
    import pandas as pd

    print("\n--- CAMADA GOLD ---")
    print("Iniciando processamento para a camada Gold (Agregação e Valor de Negócio)...")

    try:
        #  Lê as partições e recupera 'ano' e 'mes' das pastas
        dataset = ds.dataset(SILVER_PATH, format="parquet", partitioning="hive")
        table = dataset.to_table()
        table = table.combine_chunks()
        df = table.to_pandas()
        print(f"Registros lidos da camada Silver: {len(df)}")
    except Exception as e:
        print(f"Erro ao ler dados da camada Silver: {e}")
        return

    print("Colunas disponíveis na Silver:", df.columns.tolist())

    # Verifica se 'ano', 'mes', 'nome_orgao' e 'valor' estão no DataFrame
    required_cols = {'ano', 'mes', 'nome_orgao', 'valor'}
    if not required_cols.issubset(df.columns):
        print(f"As colunas esperadas {required_cols} não foram encontradas no dataframe Silver.")
        return

    print("Criando artefato de dados: gastos_agregados_por_orgao...")

    # Agregação por órgão, ano e mês
    df_gold = (
        df.groupby(['ano', 'mes', 'nome_orgao'], as_index=False)['valor']
        .sum()
        .rename(columns={'valor': 'total_gasto'})
    )

    print(f"Linhas agregadas na camada Gold: {len(df_gold)}")

    # --- Gravação ---
    print(f"Salvando artefato de dados na camada Gold ({GOLD_PATH})...")
    try:
        df_gold.to_parquet(
            GOLD_PATH,
            engine='pyarrow',
            compression='snappy',
            partition_cols=['ano', 'mes']
        )
        print(" Processamento para Gold concluído com sucesso")
        print(f"Dados salvos em: {GOLD_PATH}")
    except Exception as e:
        print(f"Erro ao salvar arquivo Parquet na camada Gold: {e}")


if __name__ == "__main__":
    print("--- Iniciando o Pipeline de Dados ELT (Brasil.IO) ---")

    fetch_and_save_raw_data()
    process_raw_to_bronze()
    process_bronze_to_silver()
    process_silver_to_gold()

    print("\n--- Pipeline de dados ELT concluído com sucesso! ---")
