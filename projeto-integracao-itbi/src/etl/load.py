"""
Módulo de Carregamento de Dados - Pipeline ETL
Projeto ITBI Recife 2023-2025
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, Optional

def consolidate_datasets(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Consolida múltiplos datasets em um único DataFrame.
    
    Args:
        datasets (Dict[str, pd.DataFrame]): Datasets por ano
        
    Returns:
        pd.DataFrame: Dataset consolidado
    """
    
    print("\n💾 CONSOLIDANDO DATASETS")
    print("-" * 30)
    
    # Verificar se todos os datasets têm colunas compatíveis
    all_columns = []
    for year, df in datasets.items():
        all_columns.extend(df.columns.tolist())
    
    unique_columns = set(all_columns)
    print(f"   📊 Colunas únicas encontradas: {len(unique_columns)}")
    
    # Consolidar datasets
    dataframes_list = list(datasets.values())
    df_consolidated = pd.concat(dataframes_list, ignore_index=True, sort=False)
    
    print(f"   ✅ Consolidação concluída:")
    print(f"      • Total de registros: {len(df_consolidated):,}")
    print(f"      • Total de colunas: {len(df_consolidated.columns)}")
    
    # Verificar distribuição por ano
    if 'source_year' in df_consolidated.columns:
        year_distribution = df_consolidated['source_year'].value_counts().sort_index()
        print(f"      • Distribuição por ano:")
        for year, count in year_distribution.items():
            print(f"        - {year}: {count:,} registros")
    
    return df_consolidated

def save_consolidated_data(df: pd.DataFrame, output_dir: str, 
                          filename: str = "itbi_consolidado_recife") -> str:
    """
    Salva o dataset consolidado em arquivo.
    
    Args:
        df (pd.DataFrame): Dataset consolidado
        output_dir (str): Diretório de saída
        filename (str): Nome do arquivo (sem extensão)
        
    Returns:
        str: Caminho do arquivo salvo
    """
    
    # Criar diretório se não existir
    os.makedirs(output_dir, exist_ok=True)
    
    # Gerar timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Definir caminhos dos arquivos
    csv_path = os.path.join(output_dir, f"{filename}_{timestamp}.csv")
    excel_path = os.path.join(output_dir, f"{filename}_{timestamp}.xlsx")
    
    try:
        # Salvar como CSV
        df.to_csv(csv_path, sep=';', encoding='utf-8', index=False)
        print(f"   💾 CSV salvo: {csv_path}")
        
        # Salvar como Excel (se possível)
        try:
            df.to_excel(excel_path, index=False, engine='openpyxl')
            print(f"   💾 Excel salvo: {excel_path}")
        except ImportError:
            print(f"   ⚠️ openpyxl não disponível, Excel não salvo")
        
        # Salvar versão "latest" sempre atualizada
        latest_csv = os.path.join(output_dir, f"{filename}_latest.csv")
        df.to_csv(latest_csv, sep=';', encoding='utf-8', index=False)
        print(f"   💾 Latest salvo: {latest_csv}")
        
        return csv_path
        
    except Exception as e:
        print(f"   ❌ Erro salvando arquivo: {e}")
        raise

def save_datasets_separately(datasets: Dict[str, pd.DataFrame], 
                            output_dir: str) -> Dict[str, str]:
    """
    Salva cada dataset separadamente.
    
    Args:
        datasets (Dict[str, pd.DataFrame]): Datasets por ano
        output_dir (str): Diretório de saída
        
    Returns:
        Dict[str, str]: Mapeamento ano -> caminho do arquivo
    """
    
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = {}
    
    for year, df in datasets.items():
        filename = f"itbi_{year}_processed.csv"
        filepath = os.path.join(output_dir, filename)
        
        try:
            df.to_csv(filepath, sep=';', encoding='utf-8', index=False)
            saved_files[year] = filepath
            print(f"   💾 {year}: {filepath}")
            
        except Exception as e:
            print(f"   ❌ Erro salvando {year}: {e}")
    
    return saved_files

def create_data_summary(df: pd.DataFrame) -> Dict:
    """
    Cria resumo estatístico dos dados.
    
    Args:
        df (pd.DataFrame): Dataset consolidado
        
    Returns:
        Dict: Resumo estatístico
    """
    
    summary = {
        'total_records': len(df),
        'total_columns': len(df.columns),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
        'null_values': df.isnull().sum().sum(),
        'creation_date': datetime.now().isoformat()
    }
    
    # Adicionar informações por ano se disponível
    if 'source_year' in df.columns:
        summary['years'] = df['source_year'].unique().tolist()
        summary['records_by_year'] = df['source_year'].value_counts().to_dict()
    
    # Adicionar informações de colunas numéricas
    numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    summary['numeric_columns'] = len(numeric_columns)
    
    # Adicionar informações de colunas de texto
    text_columns = df.select_dtypes(include=['object']).columns.tolist()
    summary['text_columns'] = len(text_columns)
    
    return summary

def save_metadata(df: pd.DataFrame, output_dir: str) -> str:
    """
    Salva metadados do dataset.
    
    Args:
        df (pd.DataFrame): Dataset consolidado
        output_dir (str): Diretório de saída
        
    Returns:
        str: Caminho do arquivo de metadados
    """
    
    import json
    
    summary = create_data_summary(df)
    
    # Adicionar informações das colunas
    columns_info = {}
    for col in df.columns:
        columns_info[col] = {
            'type': str(df[col].dtype),
            'null_count': int(df[col].isnull().sum()),
            'unique_values': int(df[col].nunique()) if df[col].dtype != 'object' else 'many'
        }
    
    summary['columns_info'] = columns_info
    
    # Salvar como JSON
    metadata_path = os.path.join(output_dir, 'dataset_metadata.json')
    
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"   📋 Metadados salvos: {metadata_path}")
    
    return metadata_path

if __name__ == "__main__":
    # Teste do módulo
    print("🧪 Testando módulo de carregamento...")
    
    # Criar dados de teste
    test_data1 = pd.DataFrame({
        'valor': [100000, 200000],
        'bairro': ['Boa Viagem', 'Várzea'],
        'source_year': ['2023', '2023']
    })
    
    test_data2 = pd.DataFrame({
        'valor': [150000, 250000],
        'bairro': ['Pina', 'Madalena'],
        'source_year': ['2024', '2024']
    })
    
    test_datasets = {
        '2023': test_data1,
        '2024': test_data2
    }
    
    # Testar consolidação
    consolidated = consolidate_datasets(test_datasets)
    print(f"\n📊 Teste concluído:")
    print(f"   • Registros consolidados: {len(consolidated)}")
    print(f"   • Anos: {consolidated['source_year'].unique()}")