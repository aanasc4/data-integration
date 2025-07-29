"""
Módulo de Carregamento de Dados Brutos - Pipeline ELT
Projeto ITBI Recife 2023-2025
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional

def create_database_schema(db_path: str) -> None:
    """
    Cria o schema do banco de dados para dados brutos.
    
    Args:
        db_path (str): Caminho do banco de dados
    """
    
    print("🗄️ Criando schema do banco de dados...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Tabela para dados brutos (schema flexível)
    create_raw_table = """
    CREATE TABLE IF NOT EXISTS itbi_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        -- Dados originais (como texto para preservar formato original)
        valor_avaliacao TEXT,
        bairro TEXT,
        tipo_imovel TEXT,
        data_transacao TEXT,
        logradouro TEXT,
        numero TEXT,
        complemento TEXT,
        cep TEXT,
        area_terreno TEXT,
        area_construida TEXT,
        ano_construcao TEXT,
        fracao_ideal TEXT,
        valores_financiados_sfh TEXT,
        
        -- Metadados de carregamento
        source_year TEXT,
        extraction_timestamp TEXT,
        pipeline_type TEXT,
        load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        
        -- Índices para performance
        CONSTRAINT unique_record UNIQUE (valor_avaliacao, bairro, data_transacao, source_year)
    );
    """
    
    # Tabela de metadados do carregamento
    create_metadata_table = """
    CREATE TABLE IF NOT EXISTS load_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        source_year TEXT,
        records_loaded INTEGER,
        file_source TEXT,  
        pipeline_type TEXT,
        status TEXT,
        error_message TEXT
    );
    """
    
    # Executar criação das tabelas
    cursor.execute(create_raw_table)
    cursor.execute(create_metadata_table)
    
    # Criar índices para performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_year ON itbi_raw(source_year);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bairro ON itbi_raw(bairro);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_load_timestamp ON itbi_raw(load_timestamp);")
    
    conn.commit()
    conn.close()
    
    print("   ✅ Schema criado com sucesso!")

def load_raw_dataset(df: pd.DataFrame, year: str, db_path: str) -> int:
    """
    Carrega um dataset bruto no banco de dados.
    
    Args:
        df (pd.DataFrame): Dataset bruto
        year (str): Ano do dataset
        db_path (str): Caminho do banco
        
    Returns:
        int: Número de registros carregados
    """
    
    print(f"📥 Carregando dados brutos {year} no banco...")
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Contar registros antes
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM itbi_raw WHERE source_year = ?", (year,))
        existing_records = cursor.fetchone()[0]
        
        if existing_records > 0:
            print(f"   ⚠️ Encontrados {existing_records} registros existentes para {year}")
            # Remover registros existentes do mesmo ano
            cursor.execute("DELETE FROM itbi_raw WHERE source_year = ?", (year,))
            print(f"   🗑️ Registros antigos removidos")
        
        # Carregar dados brutos (sem transformações)
        records_loaded = 0
        
        # Preparar dados para inserção (manter como texto)
        df_to_load = df.copy()
        
        # Converter todas as colunas para string para preservar formato original
        for col in df_to_load.columns:
            if col not in ['source_year', 'extraction_timestamp', 'pipeline_type']:
                df_to_load[col] = df_to_load[col].astype(str)
        
        # Carregar para o banco
        df_to_load.to_sql('itbi_raw', conn, if_exists='append', index=False, method='multi')
        
        records_loaded = len(df_to_load)
        
        # Registrar metadados do carregamento
        metadata = {
            'source_year': year,
            'records_loaded': records_loaded,
            'file_source': f'dados.recife.pe.gov.br - {year}',
            'pipeline_type': 'ELT',
            'status': 'success',
            'error_message': None
        }
        
        pd.DataFrame([metadata]).to_sql('load_metadata', conn, if_exists='append', index=False)
        
        conn.commit()
        
        print(f"   ✅ {records_loaded:,} registros carregados com sucesso")
        
        return records_loaded
        
    except Exception as e:
        # Registrar erro nos metadados
        error_metadata = {
            'source_year': year,
            'records_loaded': 0,
            'file_source': f'dados.recife.pe.gov.br - {year}',
            'pipeline_type': 'ELT',
            'status': 'error',
            'error_message': str(e)
        }
        
        pd.DataFrame([error_metadata]).to_sql('load_metadata', conn, if_exists='append', index=False)
        conn.commit()
        
        print(f"   ❌ Erro carregando {year}: {e}")
        raise
        
    finally:
        conn.close()

def load_all_raw_datasets(datasets: Dict[str, pd.DataFrame], db_path: str) -> Dict[str, int]:
    """
    Carrega todos os datasets brutos no banco.
    
    Args:
        datasets (Dict[str, pd.DataFrame]): Datasets brutos
        db_path (str): Caminho do banco
        
    Returns:
        Dict[str, int]: Registros carregados por ano
    """
    
    print("\n💾 CARREGANDO DADOS BRUTOS NO BANCO - PIPELINE ELT")
    print("-" * 50)
    
    # Criar schema se necessário
    create_database_schema(db_path)
    
    load_results = {}
    total_loaded = 0
    
    for year, df in datasets.items():
        try:
            records_loaded = load_raw_dataset(df, year, db_path)
            load_results[year] = records_loaded
            total_loaded += records_loaded
            
        except Exception as e:
            print(f"❌ Falha carregando {year}: {e}")
            load_results[year] = 0
    
    print(f"\n✅ Carregamento concluído:")
    print(f"   • Total de registros carregados: {total_loaded:,}")
    print(f"   • Datasets processados: {len(load_results)}")
    print(f"   • Banco de dados: {db_path}")
    print(f"   • Próximo passo: TRANSFORM (transformações no banco)")
    
    return load_results

def verify_loaded_data(db_path: str) -> Dict:
    """
    Verifica os dados carregados no banco.
    
    Args:
        db_path (str): Caminho do banco
        
    Returns:
        Dict: Informações dos dados carregados
    """
    
    print("\n🔍 Verificando dados carregados no banco...")
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Contagem total
        total_query = "SELECT COUNT(*) as total FROM itbi_raw"
        total_records = pd.read_sql_query(total_query, conn)['total'][0]
        
        # Contagem por ano
        year_query = "SELECT source_year, COUNT(*) as count FROM itbi_raw GROUP BY source_year ORDER BY source_year"
        year_counts = pd.read_sql_query(year_query, conn)
        
        # Informações das colunas
        columns_query = "PRAGMA table_info(itbi_raw)"
        columns_info = pd.read_sql_query(columns_query, conn)
        
        # Metadados de carregamento
        metadata_query = "SELECT * FROM load_metadata ORDER BY load_timestamp DESC LIMIT 10"
        load_metadata = pd.read_sql_query(metadata_query, conn)
        
        verification_info = {
            'total_records': total_records,
            'records_by_year': year_counts.to_dict('records'),
            'total_columns': len(columns_info),
            'recent_loads': load_metadata.to_dict('records')
        }
        
        print(f"   📊 Total de registros: {total_records:,}")
        print(f"   📅 Distribuição por ano:")
        for record in year_counts.to_dict('records'):
            print(f"      • {record['source_year']}: {record['count']:,} registros")
        
        print(f"   🗂️ Colunas no banco: {len(columns_info)}")
        
        return verification_info
        
    finally:
        conn.close()

def create_raw_data_views(db_path: str) -> None:
    """
    Cria views úteis para os dados brutos.
    
    Args:
        db_path (str): Caminho do banco
    """
    
    print("\n📋 Criando views para dados brutos...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # View de resumo por ano
        view_summary_by_year = """
        CREATE VIEW IF NOT EXISTS v_summary_by_year AS
        SELECT 
            source_year,
            COUNT(*) as total_records,
            COUNT(DISTINCT bairro) as unique_bairros,
            COUNT(DISTINCT tipo_imovel) as unique_tipos,
            MIN(load_timestamp) as first_load,
            MAX(load_timestamp) as last_load
        FROM itbi_raw 
        GROUP BY source_year
        ORDER BY source_year;
        """
        
        # View de qualidade dos dados
        view_data_quality = """
        CREATE VIEW IF NOT EXISTS v_data_quality AS
        SELECT 
            source_year,
            COUNT(*) as total_records,
            SUM(CASE WHEN valor_avaliacao IS NULL OR valor_avaliacao = '' OR valor_avaliacao = 'nan' THEN 1 ELSE 0 END) as missing_valor,
            SUM(CASE WHEN bairro IS NULL OR bairro = '' OR bairro = 'nan' THEN 1 ELSE 0 END) as missing_bairro,
            SUM(CASE WHEN data_transacao IS NULL OR data_transacao = '' OR data_transacao = 'nan' THEN 1 ELSE 0 END) as missing_data
        FROM itbi_raw
        GROUP BY source_year
        ORDER BY source_year;
        """
        
        cursor.execute(view_summary_by_year)
        cursor.execute(view_data_quality)
        
        conn.commit()
        
        print("   ✅ Views criadas com sucesso!")
        print("      • v_summary_by_year: Resumo por ano")
        print("      • v_data_quality: Qualidade dos dados")
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Teste do módulo
    print("🧪 Testando módulo ELT Load Raw...")
    
    # Criar dados de teste
    test_data1 = pd.DataFrame({
        'valor_avaliacao': ['100.000,50', '200.000,00'],
        'bairro': ['Boa Viagem', 'Várzea'],
        'tipo_imovel': ['Apartamento', 'Casa'],
        'data_transacao': ['2023-01-15', '2023-02-20'],
        'source_year': ['2023', '2023'],
        'extraction_timestamp': [datetime.now(), datetime.now()],
        'pipeline_type': ['ELT', 'ELT']
    })
    
    test_datasets = {'2023': test_data1}
    
    # Testar carregamento
    db_path = "test_elt_database.db"
    
    try:
        results = load_all_raw_datasets(test_datasets, db_path)
        verification = verify_loaded_data(db_path)
        create_raw_data_views(db_path)
        
        print(f"\n✅ Teste concluído com sucesso!")
        print(f"   • Registros carregados: {sum(results.values())}")
        
    finally:
        # Limpar arquivo de teste
        if os.path.exists(db_path):
            os.remove(db_path)