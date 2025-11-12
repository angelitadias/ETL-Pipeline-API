# 🧠 Pipeline de Dados ELT – Brasil.IO

Este projeto implementa um pipeline de dados ELT para coletar, transformar e organizar informações públicas do Brasil.IO sobre gastos diretos do governo.

## 🚀 Visão Geral

Fluxo de camadas: Raw – dados brutos extraídos da API (.json); Bronze – dados estruturados e particionados (.parquet); Silver – dados limpos e padronizados (.parquet); Gold – dados agregados prontos para BI (.parquet).

## ⚙️ Requisitos

Python 3.10+
Dependências: pip install requests pandas python-dotenv pyarrow

## 🔑 Configuração

Crie um arquivo .env na raiz do projeto com seu token do Brasil.IO:
API_TOKEN=seu_token_aqui
O token pode ser obtido no painel do Brasil.IO, em Configurações → API Token.

## ▶️ Execução

Ative o ambiente virtual e rode: python main.py
Etapas do pipeline:

1. fetch_and_save_raw_data() – coleta da API
2. process_raw_to_bronze() – consolidação e conversão para Parquet
3. process_bronze_to_silver() – limpeza e validação
4. process_silver_to_gold() – agregação final

## 📊 Regras de Qualidade

Colunas obrigatórias: ano, mes, nome_orgao, nome_favorecido, valor
mes deve estar entre 1 e 12
valor não pode ser negativo
Dados nulos são tratados
Textos padronizados

## ⚠️ Limitações

1. Limite de requisições (429 Too Many Requests) – o script aguarda 15 segundos automaticamente antes de continuar para evitar bloqueios.
2. Paginação automática – percorre todas as páginas e evita downloads duplicados.
3. Particionamento – falha controlada se colunas ano ou mes não existirem.
4. Erros de conversão – tratados com try/except.

## 🧠 Camada Gold

Gera o artefato gastos_agregados_por_orgao com o total gasto por ano, mes e nome_orgao.

## 🧪 Testes de Qualidade

Executa verificações de colunas críticas, meses válidos e valores não negativos.

## 🧰 Comandos Úteis

Criar ambiente virtual: python -m venv venv
Ativar: .\venv\Scripts\activate (Windows) ou source venv/bin/activate (Linux/Mac)
Instalar dependências: pip install -r requirements.txt
Rodar pipeline: python main.py
