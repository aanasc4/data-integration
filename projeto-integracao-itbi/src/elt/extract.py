"""
Módulo de Extração de Dados - Pipeline ELT
Projeto ITBI Recife 2023-2025
"""

import pandas as pd
import sqlite3
from datetime import datetime
from typing import Dict, List
import os

def extract_itbi_data_elt() -> Dict[str, pd.DataFrame]:
    """
    Extrai dados ITBI dos 3 anos para o pipeline ELT.
    No ELT, extraímos os dados brutos sem transformações.
    
    Returns:
        Dict[str, pd.DataFrame]: Dicionário com datasets brutos por ano
    """
    
    # URLs dos datasets oficiais
    datasets_urls = [
        ("2023", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/d0c08a6f-4c27-423c-9219-8d13403816f4/download/itbi_2023.csv"),
        ("2024", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/a36d548b-d705-496a-ac47-4ec36f068474/download/itbi_2024.csv"),
        ("2025", "http://dados.recife.pe.gov.br/dataset/28e3e25e-a9a7-4a9f-90a8-bb02d09cbc18/resource/5b582147-3935-459a-bbf7-ee623c22c97b/download/itbi_2025.csv")
    ]
    
    datasets_dict = {}
    total_records = 0
    
    print("📥 INICIANDO EXTRAÇÃO DOS DADOS - PIPELINE ELT")
    print("-" * 45)
    print("🔍 No ELT, extraímos dados BRUTOS sem transformações")
    
    for year, url in datasets_urls:
        print(f"\n📅 Extraindo dados brutos ITBI {year}...")
        
        try:
            # Carregar dados SEM transformações (dados brutos)
            df = pd.read_csv(url, sep=';', encoding='utf-8')
            
            # Apenas adicionar metadados mínimos
            df['source_year'] = year
            df['extraction_timestamp'] = datetime.now()
            df['pipeline_type'] = 'ELT'
            
            # Validação básica
            if df.empty:
                raise ValueError(f"Dataset {year} está vazio")
            
            # Armazenar dataset bruto
            datasets_dict[year] = df
            total_records += len(df)
            
            print(f"   ✅ Dados brutos extraídos: {len(df):,} registros")
            print(f"   📊 Colunas originais: {len(df.columns)}")
            print(f"   📝 Nenhuma transformação aplicada na extração")
            
        except Exception as e:
            print(f"   ❌ Erro extraindo {year}: {e}")
            continue
    
    print(f"\n✅ Extração ELT concluída:")
    print(f"   • Datasets extraídos: {len(datasets_dict)}")
    print(f"   • Total de registros brutos: {total_records:,}")
    print(f"   • Próximo passo: LOAD (carregar no banco)")
    
    return datasets_dict

def validate_raw_data(datasets: Dict[str, pd.DataFrame]) -> bool:
    """
    Valida dados brutos extraídos (validação mínima).
    
    Args:
        datasets (Dict[str, pd.DataFrame]): Datasets brutos
        
    Returns:
        bool: True se válidos para carregamento
    """
    
    print("\n🔍 Validando dados brutos para carregamento...")
    
    for year, df in datasets.items():
        # Verificações básicas
        if df.empty:
            print(f"   ❌ {year}: Dataset vazio")
            return False
        
        if len(df.columns) < 10:
            print(f"   ❌ {year}: Muito poucas colunas ({len(df.columns)})")
            return False
        
        print(f"   ✅ {year}: Dados válidos para carregamento ({len(df):,} registros)")
    
    print("   🎯 Todos os datasets prontos para LOAD")
    return True

def save_raw_data_backup(datasets: Dict[str, pd.DataFrame], backup_dir: str = "data/raw") -> Dict[str, str]:
    """
    Salva backup dos dados brutos extraídos.
    
    Args:
        datasets (Dict[str, pd.DataFrame]): Datasets brutos
        backup_dir (str): Diretório para backup
        
    Returns:
        Dict[str, str]: Mapeamento ano -> arquivo de backup
    """
    
    os.makedirs(backup_dir, exist_ok=True)
    
    backup_files = {}
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    print(f"\n💾 Salvando backup dos dados brutos...")
    
    for year, df in datasets.items():
        filename = f"itbi_{year}_raw_backup_{timestamp}.csv"
        filepath = os.path.join(backup_dir, filename)
        
        try:
            df.to_csv(filepath, sep=';', encoding='utf-8', index=False)
            backup_files[year] = filepath
            print(f"   💾 {year}: {filepath}")
            
        except Exception as e:
            print(f"   ❌ Erro salvando backup {year}: {e}")
    
    return backup_files

def get_raw_data_summary(datasets: Dict[str, pd.DataFrame]) -> Dict:
    """
    Gera resumo dos dados brutos extraídos.
    
    Args:
        datasets (Dict[str, pd.DataFrame]): Datasets brutos
        
    Returns:
        Dict: Resumo dos dados brutos
    """
    
    summary = {
        'extraction_timestamp': datetime.now().isoformat(),
        'pipeline_type': 'ELT',
        'total_datasets': len(datasets),
        'datasets_info': {}
    }
    
    total_records = 0
    
    for year, df in datasets.items():
        dataset_info = {
            'records': len(df),
            'columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'null_values': df.isnull().sum().sum(),
            'column_names': df.columns.tolist()
        }
        
        summary['datasets_info'][year] = dataset_info
        total_records += len(df)
    
    summary['total_records'] = total_records
    
    return summary

if __name__ == "__main__":
    # Teste do módulo ELT Extract
    print("🧪 Testando módulo ELT Extract...")
    
    # Simular extração
    datasets = extract_itbi_data_elt()
    
    if validate_raw_data(datasets):
        print("\n✅ Teste de extração ELT bem-sucedido!")
        
        # Mostrar resumo
        summary = get_raw_data_summary(datasets)
        print(f"\n📊 Resumo da extração:")
        print(f"   • Total de registros: {summary['total_records']:,}")
        print(f"   • Datasets: {summary['total_datasets']}")
        
        # Salvar backup
        backup_files = save_raw_data_backup(datasets)
        
    else:
        print("\n❌ Falha na validação dos dados brutos")