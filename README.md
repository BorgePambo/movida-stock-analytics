# 🚗 Movida Stock Analytics
## Data Engineering Pipeline + Analytics Dashboard

Pipeline completo de Engenharia de Dados responsável pela coleta, processamento e análise do estoque de veículos disponíveis no site **Seminovos Movida**, aplicando conceitos modernos de Data Lake, ETL e Analytics.

O projeto implementa um fluxo **end-to-end**:

<img width="1536" height="1024" alt="iamgeme925" src="https://github.com/user-attachments/assets/02f121e1-bb2f-4264-958a-243e76b45e4f" />



---

# 📌 Visão Geral

A diretoria solicitou um estudo analítico sobre o estoque atual de veículos disponíveis para compra online.

Este projeto realiza:

✅ Coleta automatizada dos anúncios  
✅ Tratamento e padronização dos dados  
✅ Armazenamento em arquitetura de camadas  
✅ Modelagem analítica  
✅ Criação de KPIs e Dashboard executivo  

Fonte dos dados:

https://www.seminovosmovida.com.br/busca

---

# 🏗️ Arquitetura do Projeto




---

# 🧱 Arquitetura de Dados (Medallion Architecture)

O projeto utiliza o padrão moderno **Medallion Architecture**.

## 🥉 Bronze — Raw Data
- Dados extraídos diretamente do site
- Sem transformação
- Armazenados em JSON
- Representa a fonte original

**Objetivo:** rastreabilidade e reprocessamento.

---

## 🥈 Silver — Clean Data
- Dados limpos e normalizados
- Conversão de preços e KM
- Parsing de atributos
- Remoção de duplicados

Formato: CSV

**Objetivo:** dados confiáveis para análise.

---

## 🥇 Gold — Analytics Data
- Dados agregados
- Modelagem analítica
- KPIs calculados

Formato:
- DuckDB (banco analítico)
- Parquet (alta performance)

**Objetivo:** consumo por BI.

---

# ⚙️ Tecnologias Utilizadas

| Tecnologia | Função |
|------------|--------|
| Python 3.11 | Linguagem principal |
| Playwright | Web scraping automatizado |
| Pandas | Transformação de dados |
| DuckDB | Banco analítico local |
| Power BI | Visualização e KPIs |
| JSON / CSV / Parquet | Formatos de armazenamento |

---

# 📂 Estrutura do Repositório

      movida-stock-analytics/
      │
      ├── data/
      │ ├── bronze/ # dados brutos
      │ ├── silver/ # dados tratados
      │ └── gold/ # dados analíticos
      │
      ├── src/
      │ ├── extract.py
      │ ├── transform.py
      │ └── load.py
      │
      ├── dashboard/
      │ └── dashboard.pbix
      │
      ├── main.py
      ├── requirements.txt
      ├── .gitignore
      └── README.md


---

# 🔄 Pipeline de Dados (ETL)

## 1️⃣ Extract — Web Scraping

Responsável por:

- Navegar automaticamente no site
- Scroll infinito da página
- Coletar todos anúncios
- Extrair atributos dos veículos

Campos coletados:

- Marca
- Modelo
- Versão
- Preço atual
- Preço antigo
- Quilometragem
- Estado
- Informações do anúncio

data/bronze/*.json


---

## 2️⃣ Transform — Data Processing

Processos aplicados:

- Limpeza de strings monetárias
- Conversão para tipos numéricos
- Padronização de colunas
- Deduplicação
- Normalização dos dados

Saída:

data/silver/*.csv


---

## 3️⃣ Load — Analytics Layer

Executado com DuckDB:

Cria tabelas:

- `cars_raw`
- `cars_gold`

KPIs calculados:

- Total de veículos
- Preço médio
- KM médio
- Total em promoção

Exportação:
data/gold/cars_analytics.duckdb
data/gold/cars_gold.parquet


---

# 📊 Business Intelligence (Power BI)

## Dashboard: **Movida Stock Analytics Dashboard**
<img width="1467" height="808" alt="dash_img" src="https://github.com/user-attachments/assets/209cfa3c-6e86-4721-a74e-bbd33c176fba" />

### KPIs Principais

- 🚗 Total de estoque
- 💰 Preço médio
- 🛣️ KM médio geral
- 🔥 % veículos em promoção

### Visualizações

- Estoque por modelo
- Estoque por marca
- Distribuição geográfica por estado
- Tabela analítica detalhada

Arquivo:
dashboard/dashboard.pbix


---

# ▶️ Como Executar o Projeto

## 1. Clonar repositório

```bash
git clone https://github.com/seu-usuario/movida-stock-analytics.git
cd movida-stock-analytica

2. Criar ambiente virtual
python -m venv .myvenv
Ativar ambiente
Windows

.myvenv\Scripts\activate
Linux / WSL

source .myvenv/bin/activate
3. Instalar dependências
pip install -r requirements.txt
playwright install
4. Executar Pipeline
python main.py
Fluxo automático:

Extract → Transform → Load

5. Abrir Dashboard
Abrir Power BI Desktop

Abrir o arquivo:

dashboard/dashboard.pbix
Atualizar dados.

🛠️ Troubleshooting
Problema	Solução
Navegador não inicia	executar playwright install
Sem dados Bronze	verificar conexão ou mudança no site
Silver vazio	limpar Bronze e executar novamente
Erro DuckDB	apagar arquivo .duckdb e reexecutar
📚 Conceitos de Engenharia Aplicados
Data Lake Architecture

Medallion Architecture

ETL Pipeline

Analytics Engineering

Data Modeling

Data Quality


```

👨‍💻 Autor
Borge Pambo
Data Engineer








