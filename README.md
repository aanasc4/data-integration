# 🏠 Projeto de Integração de Dados - ITBI Recife 2023-2025

## 📋 Descrição do Projeto

Este projeto implementa pipelines **ETL** (Extract, Transform, Load) e **ELT** (Extract, Load, Transform) para integração e análise de dados do **ITBI** (Imposto sobre Transmissão de Bens Imóveis) da cidade do Recife, abrangendo os anos de 2023, 2024 e 2025.

### 🎯 Objetivos

- Integrar dados de múltiplos anos em uma base consolidada
- Implementar e comparar abordagens ETL vs ELT
- Gerar insights sobre o mercado imobiliário do Recife
- Aplicar técnicas de limpeza e transformação de dados
- Criar visualizações e análises estatísticas

## 🏗️ Arquitetura do Projeto

```
projeto-integracao-itbi/
│
├── 📁 src/
│   ├── 📁 etl/                 # Pipeline ETL
│   │   ├── extract.py          # Extração de dados
│   │   ├── transform.py        # Transformações
│   │   └── load.py            # Carregamento
│   │
│   ├── 📁 elt/                 # Pipeline ELT
│   │   ├── extract.py          # Extração de dados brutos
│   │   ├── load_raw.py         # Carregamento no banco
│   │   └── transform_db.py     # Transformações no banco
│   │
│   ├── 📁 analysis/            # Módulos de análise
│   │   └── analyzer.py         # Análises e insights
│   │
│   └── 📁 utils/               # Utilitários
│       └── data_quality.py     # Qualidade dos dados
│
├── 📁 scripts/                 # Scripts de execução
│   ├── run_etl.py             # Executar pipeline ETL
│   └── run_elt.py             # Executar pipeline ELT
│
├── 📁 notebooks/               # Notebooks Jupyter/Colab
│   ├── 01_pipeline_etl.ipynb  # Demo ETL
│   └── 02_pipeline_elt.ipynb  # Demo ELT
│
├── 📁 datasets/                # Dados e banco
│   ├── itbi_consolidado_*.csv  # Dados consolidados
│   └── itbi_datawarehouse.db   # Banco de dados
│
├── 📁 docs/                    # Documentação
│   ├── diagramas/             # Diagramas dos processos
│   └── relatorio.md           # Relatório completo
│
└── 📁 results/                 # Resultados e visualizações
    ├── visualizacoes/         # Gráficos gerados
    └── insights/              # Relatórios de insights
```

## 🔄 Pipelines Implementados

### ETL (Extract, Transform, Load)
```
📥 EXTRACT → 🔄 TRANSFORM → 💾 LOAD
```

- **Extract**: Coleta dados das URLs oficiais
- **Transform**: Aplica limpeza e transformações em Python
- **Load**: Carrega dados já processados no destino

### ELT (Extract, Load, Transform)
```
📥 EXTRACT → 💾 LOAD → 🔄 TRANSFORM
```

- **Extract**: Coleta dados brutos sem transformação
- **Load**: Carrega dados brutos no banco de dados
- **Transform**: Executa transformações usando SQL no banco

## 📊 Fontes de Dados

- **Portal**: [Dados Abertos Recife](http://dados.recife.pe.gov.br)
- **Dataset**: Imposto sobre Transmissão de Bens Imóveis (ITBI)
- **Período**: 2023, 2024, 2025
- **Formato**: CSV com separador `;`
- **Encoding**: UTF-8

### URLs dos Datasets:
- **2023**: `http://dados.recife.pe.gov.br/.../itbi_2023.csv`
- **2024**: `http://dados.recife.pe.gov.br/.../itbi_2024.csv`
- **2025**: `http://dados.recife.pe.gov.br/.../itbi_2025.csv`

## 🚀 Como Executar

### Pré-requisitos

```bash
pip install pandas numpy matplotlib seaborn sqlite3 requests
```

### 1. Pipeline ETL

```bash
# Executar pipeline ETL completo
python scripts/run_etl.py

# Ou executar individualmente
python -m src.etl.extract
python -m src.etl.transform  
python -m src.etl.load
```

### 2. Pipeline ELT

```bash
# Executar pipeline ELT completo
python scripts/run_elt.py

# Ou executar individualmente
python -m src.elt.extract
python -m src.elt.load_raw
python -m src.elt.transform_db
```

### 3. Análises

```bash
# Executar análises completas
python -m src.analysis.analyzer
```

### 4. Notebooks

- Abra os notebooks no **Google Colab** ou **Jupyter**
- Execute todas as células sequencialmente
- Os notebooks são self-contained com todas as dependências

## 📈 Análises Implementadas

### 1. 📅 Análise Temporal
- Evolução dos valores ao longo do tempo
- Tendências de crescimento/declínio
- Sazonalidade das transações
- Comparação ano a ano

### 2. 🏘️ Análise Geográfica
- Ranking de bairros por volume de transações
- Bairros mais valorizados
- Concentração geográfica do mercado
- Evolução por região

### 3. 🏢 Análise de Segmentação
- Tipos de imóveis mais transacionados
- Distribuição por faixa de valor
- Correlação entre características
- Padrões de financiamento

## 🎯 Principais Insights

### 💰 Mercado Imobiliário
- **Valor médio geral**: R$ XXX.XXX,XX
- **Crescimento no período**: +XX,X%
- **Bairro mais ativo**: [Nome do bairro]
- **Tipo predominante**: [Tipo de imóvel]

### 🏘️ Concentração Geográfica
- **Top 5 bairros**: XX,X% das transações
- **Top 10 bairros**: XX,X% das transações
- **Total de bairros ativos**: XXX

### 💳 Financiamento
- **Percentual com financiamento**: XX,X%
- **Valor médio financiado**: R$ XXX.XXX,XX

## ⚖️ Comparativo ETL vs ELT

| Aspecto | ETL | ELT |
|---------|-----|-----|
| **Ordem** | Extract → Transform → Load | Extract → Load → Transform |
| **Transformações** | Em Python (memória) | No banco de dados (SQL) |
| **Flexibilidade** | Menor | Maior |
| **Performance** | Boa para volumes pequenos | Melhor para grandes volumes |
| **Auditoria** | Limitada | Completa (dados brutos preservados) |
| **Manutenção** | Requer reprocessamento | Transformações independentes |

### 🏆 Quando Usar Cada Um?

**Use ETL quando:**
- Volume de dados pequeno/médio
- Transformações bem definidas
- Implementação rápida necessária
- Equipe experiente em Python

**Use ELT quando:**
- Volume de dados grande
- Necessita flexibilidade futura
- Dados brutos devem ser preservados
- Equipe experiente em SQL/Banco

## 📊 Resultados e Artefatos

### Arquivos Gerados
- `itbi_consolidado_YYYYMMDD_HHMMSS.csv` - Dados consolidados (ETL)
- `itbi_datawarehouse.db` - Banco de dados completo (ELT)
- `dataset_metadata.json` - Metadados dos datasets
- Visualizações em PNG/PDF

### Tabelas no Banco (ELT)
- `itbi_raw` - Dados brutos preservados
- `itbi_transformed` - Dados transformados
- `itbi_metrics` - Métricas agregadas
- `load_metadata` - Metadados de carregamento

## 👥 Equipe e Contribuições

Este projeto demonstra:
- ✅ Implementação de pipelines ETL e ELT
- ✅ Integração de dados multi-temporais
- ✅ Análises estatísticas avançadas
- ✅ Visualizações informativas
- ✅ Documentação completa
- ✅ Código bem estruturado

## 🔧 Tecnologias Utilizadas

- **Python 3.8+**
- **Pandas** - Manipulação de dados
- **SQLite** - Banco de dados
- **Matplotlib/Seaborn** - Visualizações
- **Jupyter/Google Colab** - Notebooks
- **Git** - Controle de versão

## 📝 Como Contribuir

1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para detalhes.

## 🔗 Links Úteis

- [Portal de Dados Abertos do Recife](http://dados.recife.pe.gov.br)
- [Documentação Pandas](https://pandas.pydata.org/docs/)
- [SQLite Documentation](https://sqlite.org/docs.html)
- [Matplotlib Gallery](https://matplotlib.org/stable/gallery/)

---

**📧 Contato**: [seu-email@exemplo.com]  
**📅 Última atualização**: Julho 2025