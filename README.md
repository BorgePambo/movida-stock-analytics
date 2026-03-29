# 🚗 Movida Stock Analytics
## Data Engineering Pipeline + Analytics Dashboard

Pipeline completo de Engenharia de Dados responsável pela coleta, processamento e análise do estoque de veículos disponíveis no site **Seminovos Movida**, aplicando conceitos modernos de Data Lake, ETL e Analytics.

O projeto implementa um fluxo **end-to-end**:




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

Saída:


## Arquitetura Geral

