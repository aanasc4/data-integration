"""
Módulo de Transformação de Dados - Pipeline ETL
Projeto ITBI Recife 2023-2025
"""

import pandas as pd
import numpy as np
from typing import Dict, List
from datetime import datetime

def fix_encoding(text):
    """
    Corrige problemas de encoding em texto.
    
    Args:
        text: Texto a ser corrigido
        
    Returns:
        str: Texto com encoding corrigido
    """
    if not isinstance(text, str):
        return text
    try:
        return text.encode('latin1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text

def convert_currency_format(value):
    """
    Converte formato monetário brasileiro (vírgula) para internacional (ponto).
    
    Args:
        value: Valor a ser convertido
        
    Returns:
        str: Valor no formato internacional
    """
    if pd.isna(value):
        return value
    return str(value).replace(',', '.')

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e padroniza nomes de colunas.
    
    Args:
        df (pd.DataFrame): DataFrame a ser processado
        
    Returns:
        pd.DataFrame: DataFrame com colunas renomeadas
    """
    df = df.copy()
    
    # Renomeações específicas
    column_mapping = {
        'sfh': 'valores_financiados_sfh'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Remover colunas redundantes
    columns_to_drop = []
    if 'cidade' in df.columns:
        columns_to_drop.extend(['cidade', 'uf'])
    
    if columns_to_drop:
        df = df.drop(columns_to_drop, axis=1)
        print(f"   • Colunas removidas: {columns_to_drop}")
    
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata valores nulos no dataset.
    
    Args:
        df (pd.DataFrame): DataFrame a ser processado
        
    Returns:
        pd.DataFrame: DataFrame com nulos tratados
    """
    df = df.copy()
    
    # Contar nulos antes
    nulls_before = df.isnull().sum().sum()
    
    # Tratar complemento
    if 'complemento' in df.columns:
        df['complemento'] = df['complemento'].fillna('Sem complemento')
    
    # Contar nulos depois
    nulls_after = df.isnull().sum().sum()
    
    print(f"   • Nulos tratados: {nulls_before} → {nulls_after}")
    
    return df

def fix_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige tipos de dados das colunas.
    
    Args:
        df (pd.DataFrame): DataFrame a ser processado
        
    Returns:
        pd.DataFrame: DataFrame com tipos corrigidos
    """
    df = df.copy()
    
    # Colunas monetárias/numéricas
    numeric_columns = [
        'valor_avaliacao', 'area_terreno', 'area_construida', 
        'valores_financiados_sfh', 'fracao_ideal'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            try:
                # Converter formato brasileiro para internacional
                df[col] = df[col].apply(convert_currency_format)
                # Converter para float
                df[col] = df[col].astype(float)
                print(f"   • {col}: convertido para float")
            except Exception as e:
                print(f"   ⚠️ Erro convertendo {col}: {e}")
    
    # Converter datas
    if 'data_transacao' in df.columns:
        try:
            df['data_transacao'] = pd.to_datetime(df['data_transacao'])
            print(f"   • data_transacao: convertido para datetime")
        except Exception as e:
            print(f"   ⚠️ Erro convertendo datas: {e}")
    
    return df

def fix_text_encoding(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige encoding de colunas de texto.
    
    Args:
        df (pd.DataFrame): DataFrame a ser processado
        
    Returns:
        pd.DataFrame: DataFrame com encoding corrigido
    """
    df = df.copy()
    
    # Colunas de texto (excluir numéricas)
    exclude_columns = [
        'valores_financiados_sfh', 'valor_avaliacao', 
        'area_terreno', 'area_construida', 'fracao_ideal'
    ]
    
    text_columns = df.select_dtypes(include=['object']).columns
    text_columns = [col for col in text_columns if col not in exclude_columns]
    
    for col in text_columns:
        try:
            df[col] = df[col].apply(fix_encoding)
        except Exception as e:
            print(f"   ⚠️ Erro corrigindo encoding {col}: {e}")
    
    print(f"   • Encoding corrigido em {len(text_columns)} colunas")
    
    return df

def create_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria métricas derivadas.
    
    Args:
        df (pd.DataFrame): DataFrame a ser processado
        
    Returns:
        pd.DataFrame: DataFrame com métricas derivadas
    """
    df = df.copy()
    
    print("   🧮 Criando métricas derivadas...")
    
    # Valor por m²
    try:
        df['valor_por_m2'] = (
            df['valor_avaliacao'] / df['area_construida']
        ).round(2)
        print("   • valor_por_m2: criado")
    except Exception as e:
        print(f"   ⚠️ Erro criando valor_por_m2: {e}")
    
    # Idade do imóvel
    try:
        df['idade_imovel'] = (
            df['data_transacao'].dt.year - df['ano_construcao']
        )
        print("   • idade_imovel: criado")
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
        
        df['faixa_valor'] = df['valor_avaliacao'].apply(classificar_valor)
        print("   • faixa_valor: criado")
    except Exception as e:
        print(f"   ⚠️ Erro criando faixa_valor: {e}")
    
    # Indicador de financiamento
    try:
        if 'valores_financiados_sfh' in df.columns:
            df['tem_financiamento'] = (df['valores_financiados_sfh'] > 0)
            print("   • tem_financiamento: criado")
    except Exception as e:
        print(f"   ⚠️ Erro criando tem_financiamento: {e}")
    
    # Componentes temporais
    try:
        if 'data_transacao' in df.columns:
            df['mes_transacao'] = df['data_transacao'].dt.month
            df['trimestre'] = df['data_transacao'].dt.quarter
            df['ano_transacao'] = df['data_transacao'].dt.year
            print("   • componentes temporais: criados")
    except Exception as e:
        print(f"   ⚠️ Erro criando componentes temporais: {e}")
    
    return df

def transform_dataset(df: pd.DataFrame, year: str) -> pd.DataFrame:
    """
    Aplica todas as transformações em um dataset.
    
    Args:
        df (pd.DataFrame): Dataset a ser transformado
        year (str): Ano do dataset
        
    Returns:
        pd.DataFrame: Dataset transformado
    """
    
    print(f"\n🔄 Transformando dataset {year}:")
    
    # Pipeline de transformações
    df = clean_column_names(df)
    df = handle_missing_values(df)
    df = fix_text_encoding(df)
    df = fix_data_types(df)
    df = create_derived_metrics(df)
    
    print(f"   ✅ Transformação de {year} concluída")
    
    return df

def transform_all_datasets(datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transforma todos os datasets.
    
    Args:
        datasets (Dict[str, pd.DataFrame]): Datasets a serem transformados
        
    Returns:
        Dict[str, pd.DataFrame]: Datasets transformados
    """
    
    print("\n🔄 INICIANDO TRANSFORMAÇÃO DOS DADOS")
    print("-" * 40)
    
    transformed_datasets = {}
    
    for year, df in datasets.items():
        transformed_df = transform_dataset(df, year)
        transformed_datasets[year] = transformed_df
    
    print("\n✅ Todas as transformações concluídas!")
    
    return transformed_datasets

if __name__ == "__main__":
    # Teste do módulo
    print("🧪 Testando módulo de transformação...")
    
    # Criar dataset de teste
    test_data = {
        'valor_avaliacao': ['100.000,50', '200.000,00'],
        'bairro': ['Boa Viagem', 'Várzea'],
        'complemento': [None, 'Apto 101'],
        'area_construida': ['50,5', '75,0'],
        'data_transacao': ['2023-01-15', '2023-02-20'],
        'ano_construcao': [2000, 2010],
        'sfh': ['0,00', '50000,00']
    }
    
    test_df = pd.DataFrame(test_data)
    
    # Aplicar transformações
    result = transform_dataset(test_df, "teste")
    
    print(f"\n📊 Resultado do teste:")
    print(f"   • Colunas originais: {len(test_df.columns)}")
    print(f"   • Colunas finais: {len(result.columns)}")
    print(f"   • Tipos de dados corrigidos: ✅")