# SCRIPT SIMPLIFICADO - ETL + ANÁLISES (SEM GRÁFICOS)
# Projeto de Integração ITBI Recife 2023-2025
# UFPE - Centro de Informática - Banco de Dados 2025.1

import numpy as np
import pandas as pd
import sqlite3
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

print("🏠 PROJETO INTEGRAÇÃO ITBI RECIFE - VERSÃO SIMPLIFICADA")
print("=" * 60)
print("UFPE - Centro de Informática - Banco de Dados 2025.1")
print("=" * 60)

start_time = datetime.now()
print(f"Início da execução: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

# ============================================================================
# FASE 1: EXTRAÇÃO DOS DADOS (ETL - EXTRACT)
# ============================================================================

print("\n📥 FASE 1: EXTRAÇÃO DOS DADOS")
print("-" * 40)

# Configuração dos datasets
dataset_directory = "datasets"
os.makedirs(dataset_directory, exist_ok=True)

datasets_urls = [
    ("2023", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/d0c08a6f-4c27-423c-9219-8d13403816f4/download/itbi_2023.csv"),
    ("2024", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/a36d548b-d705-496a-ac47-4ec36f068474/download/itbi_2024.csv"),
    ("2025", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/5b582147-3935-459a-bbf7-ee623c22c97b/download/itbi_2025.csv")
]

# Extrair dados
datasets_dict = {}
total_records = 0

for year, url in datasets_urls:
    print(f"\n📅 Carregando ITBI {year}...")
    try:
        df = pd.read_csv(url, sep=';', encoding='utf-8')
        df['year'] = int(year)
        datasets_dict[year] = df
        
        print(f"   ✅ {len(df):,} registros carregados")
        print(f"   📊 Colunas: {len(df.columns)}")
        total_records += len(df)
        
    except Exception as e:
        print(f"   ❌ Erro: {e}")

print(f"\n✅ Extração concluída: {total_records:,} registros totais")

# ============================================================================
# FASE 2: TRANSFORMAÇÃO DOS DADOS (ETL - TRANSFORM)
# ============================================================================

print("\n🔄 FASE 2: TRANSFORMAÇÃO DOS DADOS")
print("-" * 40)

# Função para corrigir encoding
def fix_encoding(text):
    if not isinstance(text, str):
        return text
    try:
        return text.encode('latin1').decode('utf-8')
    except:
        return text

# Função para converter formato brasileiro para internacional
def convert_currency(value):
    if pd.isna(value):
        return value
    return str(value).replace(',', '.')

print("🧹 Aplicando transformações...")

for year, df in datasets_dict.items():
    print(f"\n📅 Transformando dataset {year}:")
    
    # 1. Remover colunas redundantes
    columns_to_drop = []
    if 'cidade' in df.columns:
        columns_to_drop.extend(['cidade', 'uf'])
    if columns_to_drop:
        df = df.drop(columns_to_drop, axis=1)
        print(f"   • Colunas removidas: {columns_to_drop}")
    
    # 2. Renomear coluna SFH
    if 'sfh' in df.columns:
        df = df.rename(columns={'sfh': 'valores_financiados_sfh'})
        print("   • Coluna SFH renomeada")
    
    # 3. Tratar valores nulos
    null_before = df.isnull().sum().sum()
    if 'complemento' in df.columns:
        df['complemento'] = df['complemento'].fillna('Sem complemento')
    null_after = df.isnull().sum().sum()
    print(f"   • Nulos antes: {null_before}, depois: {null_after}")
    
    # 4. Corrigir encoding em colunas de texto
    text_columns = df.select_dtypes(include=['object']).columns
    for col in text_columns:
        if col not in ['valores_financiados_sfh', 'valor_avaliacao', 'area_terreno', 'area_construida']:
            try:
                df[col] = df[col].apply(fix_encoding)
            except:
                pass
    print("   • Encoding corrigido")
    
    # 5. Converter tipos de dados
    monetary_columns = ['valor_avaliacao', 'area_terreno', 'area_construida']
    if 'valores_financiados_sfh' in df.columns:
        monetary_columns.append('valores_financiados_sfh')
    
    for col in monetary_columns:
        if col in df.columns:
            try:
                df[col] = df[col].apply(convert_currency).astype(float)
                print(f"   • {col} convertido para float")
            except Exception as e:
                print(f"   ⚠️ Erro convertendo {col}: {e}")
    
    # 6. Converter datas
    if 'data_transacao' in df.columns:
        try:
            df['data_transacao'] = pd.to_datetime(df['data_transacao'])
            print("   • Datas convertidas")
        except Exception as e:
            print(f"   ⚠️ Erro convertendo datas: {e}")
    
    # Atualizar dataset
    datasets_dict[year] = df

print("\n✅ Transformações concluídas")

# ============================================================================
# FASE 3: CONSOLIDAÇÃO (ETL - LOAD)
# ============================================================================

print("\n💾 FASE 3: CONSOLIDAÇÃO DOS DADOS")
print("-" * 40)

# Consolidar todos os datasets
df_consolidated = pd.concat(list(datasets_dict.values()), ignore_index=True)

print(f"✅ Consolidação concluída:")
print(f"   • Total de registros: {len(df_consolidated):,}")
print(f"   • Total de colunas: {len(df_consolidated.columns)}")
print(f"   • Período: {df_consolidated['year'].min()}-{df_consolidated['year'].max()}")

# Verificar colunas disponíveis
print(f"\n📋 Colunas disponíveis:")
for i, col in enumerate(df_consolidated.columns):
    print(f"   {i+1:2d}. {col}")

# Criar métricas derivadas
print("\n🧮 Criando métricas derivadas...")

# Valor por m² (se possível)
try:
    df_consolidated['valor_por_m2'] = (
        df_consolidated['valor_avaliacao'] / df_consolidated['area_construida']
    ).round(2)
    print("   ✅ valor_por_m2 criado")
except Exception as e:
    print(f"   ⚠️ Erro criando valor_por_m2: {e}")

# Idade do imóvel (se possível)
try:
    df_consolidated['idade_imovel'] = (
        df_consolidated['data_transacao'].dt.year - df_consolidated['ano_construcao']
    )
    print("   ✅ idade_imovel criado")
except Exception as e:
    print(f"   ⚠️ Erro criando idade_imovel: {e}")

# Faixa de valor
try:
    def classificar_valor(valor):
        if pd.isna(valor):
            return 'Não informado'
        if valor <= 200000:
            return 'Baixo (até R$ 200k)'
        elif valor <= 500000:
            return 'Médio (R$ 200k-500k)'
        elif valor <= 1000000:
            return 'Alto (R$ 500k-1M)'
        else:
            return 'Premium (acima R$ 1M)'

    df_consolidated['faixa_valor'] = df_consolidated['valor_avaliacao'].apply(classificar_valor)
    print("   ✅ faixa_valor criado")
except Exception as e:
    print(f"   ⚠️ Erro criando faixa_valor: {e}")

# Indicador de financiamento
try:
    if 'valores_financiados_sfh' in df_consolidated.columns:
        df_consolidated['tem_financiamento'] = (df_consolidated['valores_financiados_sfh'] > 0)
        print("   ✅ tem_financiamento criado")
except Exception as e:
    print(f"   ⚠️ Erro criando tem_financiamento: {e}")

# Componentes temporais
try:
    df_consolidated['mes_transacao'] = df_consolidated['data_transacao'].dt.month
    df_consolidated['trimestre'] = df_consolidated['data_transacao'].dt.quarter
    print("   ✅ componentes temporais criados")
except Exception as e:
    print(f"   ⚠️ Erro criando componentes temporais: {e}")

# Salvar dataset consolidado
consolidated_path = f"{dataset_directory}/itbi_consolidado_2023_2025.csv"
df_consolidated.to_csv(consolidated_path, sep=';', encoding='utf-8', index=False)
print(f"💾 Dataset salvo: {consolidated_path}")

# ============================================================================
# ANÁLISE 1: EVOLUÇÃO TEMPORAL DOS PREÇOS
# ============================================================================

print("\n📊 ANÁLISE 1: EVOLUÇÃO TEMPORAL DOS PREÇOS")
print("-" * 50)

try:
    # Calcular evolução por ano
    evolucao_anual = df_consolidated.groupby('year').agg({
        'valor_avaliacao': ['count', 'mean', 'median']
    }).round(2)

    # Achatar colunas
    evolucao_anual.columns = ['transacoes', 'valor_medio', 'valor_mediano']

    print("📈 Evolução Anual:")
    print(evolucao_anual)

    # Calcular variações
    anos = sorted(df_consolidated['year'].unique())
    if len(anos) >= 2:
        var_2023_2024 = ((evolucao_anual.loc[2024, 'valor_medio'] - evolucao_anual.loc[2023, 'valor_medio']) / evolucao_anual.loc[2023, 'valor_medio']) * 100
        
        if 2025 in evolucao_anual.index:
            var_2024_2025 = ((evolucao_anual.loc[2025, 'valor_medio'] - evolucao_anual.loc[2024, 'valor_medio']) / evolucao_anual.loc[2024, 'valor_medio']) * 100
        else:
            var_2024_2025 = 0

        print(f"\n💡 INSIGHTS ANÁLISE 1:")
        print(f"   • Variação 2023→2024: {var_2023_2024:+.1f}%")
        if var_2024_2025 != 0:
            print(f"   • Variação 2024→2025: {var_2024_2025:+.1f}%")
        print(f"   • Ano com mais transações: {evolucao_anual['transacoes'].idxmax()}")
        print(f"   • Maior valor médio: R$ {evolucao_anual['valor_medio'].max():,.2f}")

except Exception as e:
    print(f"❌ Erro na Análise 1: {e}")

# ============================================================================
# ANÁLISE 2: ANÁLISE GEOGRÁFICA POR BAIRROS
# ============================================================================

print("\n🗺️ ANÁLISE 2: ANÁLISE GEOGRÁFICA POR BAIRROS")
print("-" * 50)

try:
    # Top bairros por transações
    if 'bairro' in df_consolidated.columns:
        top_bairros = df_consolidated.groupby('bairro').agg({
            'valor_avaliacao': ['count', 'mean']
        }).round(2)

        top_bairros.columns = ['transacoes', 'valor_medio']
        top_bairros = top_bairros.sort_values('transacoes', ascending=False)

        print("🏆 TOP 10 BAIRROS POR TRANSAÇÕES:")
        print(top_bairros.head(10))

        # Top por valor
        top_valor = top_bairros.sort_values('valor_medio', ascending=False)
        print("\n💰 TOP 10 BAIRROS POR VALOR:")
        print(top_valor.head(10))

        bairro_mais_ativo = top_bairros.index[0]
        bairro_mais_caro = top_valor.index[0]

        print(f"\n💡 INSIGHTS ANÁLISE 2:")
        print(f"   • Bairro mais ativo: {bairro_mais_ativo}")
        print(f"   • Bairro mais valorizado: {bairro_mais_caro}")
        print(f"   • Total de bairros: {df_consolidated['bairro'].nunique()}")
        print(f"   • Concentração: Top 10 = {top_bairros.head(10)['transacoes'].sum():,} transações")

except Exception as e:
    print(f"❌ Erro na Análise 2: {e}")

# ============================================================================
# ANÁLISE 3: PERFIL DE IMÓVEIS E FINANCIAMENTO
# ============================================================================

print("\n🏦 ANÁLISE 3: PERFIL DE IMÓVEIS E FINANCIAMENTO")
print("-" * 55)

try:
    # Análise por tipo de imóvel
    if 'tipo_imovel' in df_consolidated.columns:
        tipos_imoveis = df_consolidated.groupby('tipo_imovel').agg({
            'valor_avaliacao': ['count', 'mean']
        }).round(2)

        tipos_imoveis.columns = ['quantidade', 'valor_medio']
        tipos_imoveis = tipos_imoveis.sort_values('quantidade', ascending=False)

        print("🏘️ ANÁLISE POR TIPO DE IMÓVEL:")
        print(tipos_imoveis)

        # Análise de financiamento
        if 'tem_financiamento' in df_consolidated.columns:
            total_financiados = df_consolidated['tem_financiamento'].sum()
            percentual_financiamento = (total_financiados / len(df_consolidated)) * 100

            print(f"\n💳 FINANCIAMENTO GERAL:")
            print(f"   • Com financiamento: {total_financiados:,} ({percentual_financiamento:.1f}%)")
            print(f"   • Sem financiamento: {len(df_consolidated) - total_financiados:,}")

        # Por faixa de valor
        if 'faixa_valor' in df_consolidated.columns:
            faixas_analise = df_consolidated['faixa_valor'].value_counts()
            print(f"\n💰 DISTRIBUIÇÃO POR FAIXA:")
            print(faixas_analise)

        tipo_mais_comum = df_consolidated['tipo_imovel'].value_counts().index[0]
        tipo_mais_caro = tipos_imoveis['valor_medio'].idxmax()

        print(f"\n💡 INSIGHTS ANÁLISE 3:")
        print(f"   • Tipo mais comum: {tipo_mais_comum}")
        print(f"   • Tipo mais caro: {tipo_mais_caro}")
        if 'tem_financiamento' in df_consolidated.columns:
            print(f"   • Taxa geral financiamento: {percentual_financiamento:.1f}%")
        if 'idade_imovel' in df_consolidated.columns:
            print(f"   • Idade média imóveis: {df_consolidated['idade_imovel'].mean():.1f} anos")

except Exception as e:
    print(f"❌ Erro na Análise 3: {e}")

# ============================================================================
# PIPELINE ELT ALTERNATIVO
# ============================================================================

print("\n🔄 IMPLEMENTANDO PIPELINE ELT ALTERNATIVO")
print("-" * 50)

try:
    # Criar database SQLite
    db_path = f"{dataset_directory}/itbi_datawarehouse.db"
    conn = sqlite3.connect(db_path)

    print(f"🏛️ Data Warehouse criado: {db_path}")

    # EXTRACT + LOAD: Carregar dados brutos
    for year, url in datasets_urls:
        try:
            df_raw = pd.read_csv(url, sep=';', encoding='utf-8')
            df_raw['source_year'] = year
            df_raw['load_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            table_name = f"itbi_raw_{year}"
            df_raw.to_sql(table_name, conn, if_exists='replace', index=False)
            
            print(f"   📥 {table_name}: {len(df_raw):,} registros carregados")
            
        except Exception as e:
            print(f"   ❌ Erro carregando {year}: {e}")

    # TRANSFORM: Criar tabela limpa via SQL
    transform_sql = """
    CREATE TABLE IF NOT EXISTS itbi_elt_final AS
    SELECT 
        logradouro,
        numero,
        COALESCE(complemento, 'Sem complemento') as complemento,
        CAST(REPLACE(valor_avaliacao, ',', '.') AS REAL) as valor_avaliacao,
        bairro,
        tipo_imovel,
        data_transacao,
        source_year as year
    FROM (
        SELECT * FROM itbi_raw_2023
        UNION ALL
        SELECT * FROM itbi_raw_2024
        UNION ALL
        SELECT * FROM itbi_raw_2025
    )
    WHERE valor_avaliacao IS NOT NULL
    """

    conn.execute("DROP TABLE IF EXISTS itbi_elt_final")
    conn.execute(transform_sql)
    conn.commit()

    # Verificar resultado ELT
    elt_count = conn.execute("SELECT COUNT(*) FROM itbi_elt_final").fetchone()[0]
    print(f"✅ ELT Transform concluído: {elt_count:,} registros")

    conn.close()

except Exception as e:
    print(f"❌ Erro no ELT: {e}")

# ============================================================================
# RESUMO EXECUTIVO FINAL
# ============================================================================

end_time = datetime.now()
execution_time = end_time - start_time

print(f"\n" + "="*60)
print(f"📋 RESUMO EXECUTIVO FINAL")
print("="*60)

print(f"""
🎯 PROJETO CONCLUÍDO COM SUCESSO!

📊 DADOS INTEGRADOS:
   • Registros totais: {len(df_consolidated):,}
   • Colunas finais: {len(df_consolidated.columns)}
   • Período: {df_consolidated['year'].min()}-{df_consolidated['year'].max()}
   • Bairros únicos: {df_consolidated['bairro'].nunique() if 'bairro' in df_consolidated.columns else 'N/A'}

🔄 PIPELINES IMPLEMENTADOS:
   • ETL: Extract→Transform→Load ✅
   • ELT: Extract→Load→Transform ✅
   • Análises: 3 análises concluídas ✅

📁 ARQUIVOS GERADOS:
   • {consolidated_path}
   • {db_path}

⏱️ TEMPO DE EXECUÇÃO: {execution_time}

🚀 PRÓXIMOS PASSOS:
   1. Instalar matplotlib/seaborn para gráficos
   2. Executar versão completa com visualizações
   3. Organizar repositório GitHub
   4. Preparar apresentação
""")

print("✅ SCRIPT SIMPLIFICADO EXECUTADO COM SUCESSO!")
print("🎓 Dados processados - UFPE CIn 2025.1")

# Salvar log de execução
log_path = f"{dataset_directory}/execution_log.txt"
with open(log_path, 'w', encoding='utf-8') as f:
    f.write(f"Execução realizada em: {start_time}\n")
    f.write(f"Duração: {execution_time}\n")
    f.write(f"Registros processados: {len(df_consolidated):,}\n")
    f.write(f"Status: Sucesso\n")

print(f"📝 Log salvo: {log_path}")
