# SCRIPT PARA CRIAR A ESTRUTURA REAL DO PROJETO
# Execute este script para organizar tudo profissionalmente

echo "🏗️ CRIANDO ESTRUTURA PROFISSIONAL DO PROJETO"
echo "============================================="

# Criar diretório principal
mkdir -p projeto-integracao-itbi
cd projeto-integracao-itbi

echo "📁 Criando estrutura de diretórios..."

# Estrutura principal
mkdir -p data/{raw,processed,external}
mkdir -p notebooks
mkdir -p src/{etl,elt,analysis,utils}
mkdir -p docs/{diagramas}
mkdir -p tests
mkdir -p scripts
mkdir -p config
mkdir -p results/{datasets,analysis,visualizations}

echo "✅ Estrutura de diretórios criada!"

echo "📝 Criando arquivos de configuração..."

# .gitignore
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Jupyter
.ipynb_checkpoints
profile_default/
ipython_config.py

# Environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# Projeto específico
data/raw/
data/external/
*.db
*.sqlite
*.sqlite3
.DS_Store
Thumbs.db
temp/
tmp/
*.tmp
*.bak
logs/
*.log
config/secrets.yaml
config/credentials.json
EOF

# requirements.txt
cat > requirements.txt << 'EOF'
# Core data processing
pandas==2.1.4
numpy==1.24.3

# Visualization
matplotlib==3.7.2
seaborn==0.12.2

# Database
# sqlite3 (built-in)

# Jupyter
jupyter==1.0.0
notebook==7.0.6

# Development
pytest==7.4.2

# Utility
tqdm==4.66.1
python-dotenv==1.0.0
pyyaml==6.0.1
EOF

# README.md principal
cat > README.md << 'EOF'
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
EOF

echo "📦 Criando arquivos __init__.py..."

# Criar __init__.py em todos os módulos Python
touch src/__init__.py
touch src/etl/__init__.py
touch src/elt/__init__.py
touch src/analysis/__init__.py
touch src/utils/__init__.py
touch tests/__init__.py

echo "✅ Estrutura básica criada!"
echo ""
echo "📋 PRÓXIMOS PASSOS:"
echo "1. cd projeto-integracao-itbi"
echo "2. Execute os scripts que vou criar para popular os módulos"
echo "3. Copie seus dados para results/datasets/"
echo ""
echo "🎯 Estrutura pronta para receber o código organizado!"
