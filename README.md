# ** 🎲  Pipeline de Dados ELT para Consumo da API Brasil.IO**

Este documento apresenta a especificação técnica, arquitetura e funcionamento de um pipeline de **Extração, Carga e Transformação (ELT)** destinado ao processamento do dataset **gastos-diretos** disponibilizado pela API pública Brasil.IO.
O projeto contempla desde a ingestão de dados brutos até a geração de artefatos analíticos estruturados para consumo por ferramentas de Business Intelligence (BI) e modelos de Machine Learning (ML).

---

## **1. Requisitos e Preparação do Ambiente**

### **1.1. Criação do ambiente Python**

Criar e ativar um ambiente virtual utilizando `venv`:

```bash
python -m venv .venv
source .venv/bin/activate
```

### **1.2. Instalação de dependências**

Instalar os pacotes necessários via `pip` ou `uv`:

```bash
pip install -r requirements.txt
```

### **1.3. Variáveis de ambiente**

Criar um arquivo `.env` contendo:

```
API_TOKEN=SEU_TOKEN_BRASIL_IO
```

Essa variável é utilizada para autenticação nas requisições HTTP à API.

### **1.4. Estrutura de diretórios**

Criar a seguinte hierarquia de pastas:

```
dataset/
 ├── raw/
 ├── bronze/
 ├── silver/
 └── gold/
```

---

## **2. Arquitetura Geral do Pipeline**

O pipeline segue o modelo de camadas do **Data Lakehouse**, organizado em Raw, Bronze, Silver e Gold.

### **2.1. Raw (Dados Brutos)**

* Contém todos os dados obtidos diretamente da API Brasil.IO.
* Cada página da API é salva individualmente em formato **JSON**.
* A coleta respeita:

  * limite aproximado de **1000 páginas**;
  * tratamento automático de **rate limit 429**, com espera antes de retentar.

### **2.2. Bronze (Dados Padronizados em Parquet)**

* Consolidação dos arquivos JSON da camada Raw.
* Conversão para **Parquet**, com compressão *snappy*.
* Particionamento estruturado em:

```
ano=YYYY / mes=MM
```

Essa etapa melhora interoperabilidade, performance e organização dos dados.

### **2.3. Silver (Dados Tratados e Validados)**

A camada Silver representa a primeira etapa de transformação significativa.

Transformações aplicadas:

* **Tratamento de valores nulos** (especialmente em `valor`).
* **Padronização textual** (maiúsculas, remoção de espaços excedentes).
* **Conversão de tipos numéricos** (`ano`, `mes`, `valor`).
* **Conversão de datas** quando aplicável.
* **Aplicação de regras de integridade e qualidade**:

  * colunas críticas sem valores nulos (`ano`, `mes`, `nome_orgao`, `nome_favorecido`);
  * validação de intervalo de mês (1 a 12);
  * verificação de ausência de valores monetários negativos.

Realiza-se também uma **análise exploratória básica**, incluindo:

* contagem de registros;
* número de órgãos distintos;
* faixa temporal disponível;
* valor médio dos pagamentos.

Os dados tratados são registrados em formato **Parquet particionado**, mantendo o mesmo padrão da camada Bronze.

### **2.4. Gold (Dados Agregados e Modelados)**

A camada Gold representa a camada de **serviço (Serving Layer)**, destinada ao consumo por analistas, aplicações e modelos.

São realizadas agregações orientadas a valor, por exemplo:

* total de gastos por órgão, ano e mês.

Essa etapa caracteriza a geração de **data products**, estruturados para uso imediato, em formato Parquet e com o mesmo esquema de particionamento.

---

## **3. Funcionamento do Pipeline**

O arquivo `main.py` orquestra todas as etapas:

1. **fetch_and_save_raw_data()**
   Consulta a API Brasil.IO, trata paginação e limitações, salva arquivos brutos.

2. **process_raw_to_bronze()**
   Converte e estrutura os dados brutos em Parquet particionado.

3. **process_bronze_to_silver()**
   Aplica regras de limpeza, padronização e validação de qualidade.

4. **process_silver_to_gold()**
   Gera tabelas agregadas de alto valor analítico.

Para executar:

```bash
python main.py
```

---

## **4. Considerações sobre o Ciclo de Vida dos Dados**

### **Transform (T)**

Corresponde à etapa em que os dados deixam sua forma original para assumirem um formato estruturado, limpo e útil para casos de uso *downstream*.
Transformações em lote (batch) — como neste projeto — são amplamente utilizadas em pipelines tradicionais e modernos.

### **Camada Gold e Uso Final**

A camada Gold representa a fase final do ciclo, com dados prontos para:

* análises descritivas e diagnósticas,
* modelagem preditiva,
* ingestão por ferramentas de BI,
* execução de processos de Reverse ETL.

Essa camada contém dados já agregados, coerentes e validados, otimizados para consultas rápidas e decisões de negócio.

---

## **5. Resultado Final**

Ao final da execução completa, obtém-se:

* **Raw**: dados brutos da API Brasil.IO;
* **Bronze**: Parquets estruturados e próximos do estado original;
* **Silver**: dados limpos, padronizados e aprovados em testes de qualidade;
* **Gold**: artefatos analíticos prontos para uso corporativo.

O projeto entrega um pipeline ELT robusto, modular, escalável e alinhado às melhores práticas contemporâneas de Engenharia de Dados.

---

Se desejar, posso gerar a versão em **PDF**, produzir um **diagrama da arquitetura** ou criar **exemplos de dashboards** consumindo a camada Gold.
