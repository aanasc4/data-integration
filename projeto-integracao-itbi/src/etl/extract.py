"""
Módulo de Extração de Dados - Pipeline ETL
Projeto ITBI Recife 2023-2025
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Tuple
from ..utils.data_quality import log_operation

def extract_itbi_data() -> Dict[str, pd.DataFrame]:
    """
    Extrai dados ITBI dos 3 anos (2023, 2024, 2025) das URLs oficiais.
    
    Returns:
        Dict[str, pd.DataFrame]: Dicionário com datasets por ano
    """
    
    # URLs dos datasets oficiais
    datasets_urls = [
        ("2023", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/d0c08a6f-4c27-423c-9219-8d13403816f4/download/itbi_2023.csv"),
        ("2024", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/a36d548b-d705-496a-ac47-4ec36f068474/download/itbi_2024.csv"),
        ("2025", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/5b582147-3935-459a-bbf7-ee623c22c97b/download/itbi_2025.csv")
    ]
    
    datasets_dict = {}
    total_records = 0
    
    print("📥 INICIANDO EXTRAÇÃO DOS DADOS ITBI")
    print("-" * 40)
    
    for year, url in datasets_urls:
        print(f"\n📅 Extraindo dados ITBI {year}...")
        
        try:
            # Carregar dados com parâmetros específicos
            df = pd.read_csv(url, sep=';', encoding='utf-8')
            
            # Adicionar metadados
            df['source_year'] = year
            df['extraction_date'] = datetime.now()
            df['source_url'] = url
            
            # Validações básicas
            if df.empty:
                raise ValueError(f"Dataset {year} está vazio")
            
            if len(df.columns) < 20:
                raise ValueError(f"Dataset {year} tem poucas colunas: {len(df.columns)}")
            
            # Armazenar dataset
            datasets_dict[year] = df
            total_records += len(df)
            
            print(f"   ✅ Sucesso: {len(df):,} registros extraídos")
            print(f"   📊 Colunas: {len(df.columns)}")
            
            # Log da operação
            log_operation("extract", year, len(df), "success")
            
        except Exception as e:
            print(f"   ❌ Erro extraindo {year}: {e}")
            log_operation("extract", year, 0, f"error: {e}")
            continue
    
    print(f"\n✅ Extração concluída:")
    print(f"   • Datasets extraídos: {len(datasets_dict)}")
    print(f"   • Total de registros: {total_records:,}")
    
    return datasets_dict

def extract_single_dataset(year: str, url: str) -> pd.DataFrame:
    """
    Extrai um único dataset ITBI.
    
    Args:
        year (str): Ano do dataset
        url (str): URL do dataset
        
    Returns:
        pd.DataFrame: Dataset extraído
    """
    
    print(f"📥 Extraindo dataset {year}...")
    
    try:
        df = pd.read_csv(url, sep=';', encoding='utf-8')
        df['source_year'] = year
        df['extraction_date'] = datetime.now()
        
        print(f"   ✅ {len(df):,} registros extraídos")
        return df
        
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        raise

def validate_extracted_data(datasets: Dict[str, pd.DataFrame]) -> bool:
    """
    Valida os dados extraídos.
    
    Args:
        datasets (Dict[str, pd.DataFrame]): Datasets extraídos
        
    Returns:
        bool: True se válidos, False caso contrário
    """
    
    print("\n🔍 Validando dados extraídos...")
    
    required_columns = ['valor_avaliacao', 'bairro', 'tipo_imovel', 'data_transacao']
    
    for year, df in datasets.items():
        # Verificar colunas essenciais
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"   ❌ {year}: Colunas faltando: {missing_cols}")
            return False
        
        # Verificar se não está vazio
        if df.empty:
            print(f"   ❌ {year}: Dataset vazio")
            return False
        
        print(f"   ✅ {year}: Dataset válido ({len(df):,} registros)")
    
    return True

if __name__ == "__main__":
    # Teste do módulo
    datasets = extract_itbi_data()
    if validate_extracted_data(datasets):
        print("\n🎉 Extração e validação concluídas com sucesso!")
    else:
        print("\n❌ Problemas encontrados na validação")