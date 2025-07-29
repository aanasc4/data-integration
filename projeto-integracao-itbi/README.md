# 🏠 Projeto de Integração de Dados ITBI - Recife

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-green.svg)](https://pandas.pydata.org)

## 📋 Descrição

Projeto acadêmico de integração e análise de dados do ITBI (Imposto sobre Transmissão de Bens Imóveis) da cidade do Recife, desenvolvido para a disciplina de Banco de Dados (2025.1) da UFPE.

## 🎯 Objetivos
- Integrar dados ITBI de 2023, 2024 e 2025
- Implementar pipelines ETL e ELT
- Gerar análises e insights sobre o mercado imobiliário
- Comparar abordagens de integração de dados

## 🚀 Início Rápido

### Instalação
```bash
# Clonar o repositório
git clone https://github.com/seu-usuario/projeto-integracao-itbi.git
cd projeto-integracao-itbi

# Instalar dependências
pip install -r requirements.txt
```

### Execução

#### Pipeline ETL
```bash
python scripts/run_etl.py
```

#### Pipeline ELT
```bash
python scripts/run_elt.py
```

#### Análises
```bash
jupyter notebook notebooks/04_analises_insights.ipynb
```

## 📊 Resultados Principais

### Dataset Consolidado
- **Registros**: 35.117 transações
- **Período**: 2023-2025
- **Colunas**: 27 atributos
- **Qualidade**: 98,5% completude

### Insights Descobertos
1. **Valorização**: Variação anual de preços
2. **Concentração**: 60% das transações em 10 bairros
3. **Financiamento**: 31,7% usa financiamento

## 📁 Estrutura do Projeto

```
├── notebooks/          # Jupyter notebooks para análise
├── src/                # Código fonte organizado por módulos
├── data/               # Dados em diferentes estágios
├── docs/               # Documentação e relatórios
├── results/            # Resultados e visualizações
└── tests/              # Testes automatizados
```

## 👥 Equipe

- **[Nome 1]** - ETL Pipeline, Análise Temporal
- **[Nome 2]** - ELT Pipeline, Análise Geográfica  
- **[Nome 3]** - Análise Financiamento, Documentação

## 📚 Documentação

- [Relatório Completo](docs/relatorio_completo.md)
- [Diagramas de Fluxo](docs/diagramas/)

---

**🎓 Projeto Acadêmico - UFPE CIn 2025.1**
