#!/usr/bin/env python3
"""
Script principal para executar o pipeline ELT completo
Projeto ITBI Recife 2023-2025
"""

import sys
import os
from datetime import datetime

# Adicionar src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.elt.extract import extract_itbi_data_elt, validate_raw_data
from src.elt.load_raw import load_all_raw_datasets, verify_loaded_data, create_raw_data_views
from src.elt.transform_db import transform_all_years_in_db, create_final_views

def main():
    """Executa o pipeline ELT completo."""
    
    start_time = datetime.now()
    
    print("🚀 PIPELINE ELT - PROJETO ITBI RECIFE")
    print("=" * 50)
    print(f"Início: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    print("📋 ELT = Extract → Load → Transform")
    print("   • Extract: Dados brutos sem transformação")
    print("   • Load: Carrega dados brutos no banco")
    print("   • Transform: Transformações dentro do banco")
    
    # Definir caminho do banco de dados
    db_path = "datasets/itbi_datawarehouse.db"
    os.makedirs("datasets", exist_ok=True)
    
    try:
        # FASE 1: EXTRACT
        print("\n📥 FASE 1: EXTRACT (Extração de Dados Brutos)")
        print("-" * 45)
        
        datasets_raw = extract_itbi_data_elt()
        
        if not validate_raw_data(datasets_raw):
            raise ValueError("Dados extraídos falharam na validação")
        
        print("✅ Extract ELT concluído com sucesso!")
        
        # FASE 2: LOAD  
        print("\n💾 FASE 2: LOAD (Carregamento no Banco)")
        print("-" * 40)
        
        load_results = load_all_raw_datasets(datasets_raw, db_path)
        
        # Verificar dados carregados
        verification_info = verify_loaded_data(db_path)
        
        # Criar views para dados brutos
        create_raw_data_views(db_path)
        
        print("✅ Load ELT concluído com sucesso!")
        
        # FASE 3: TRANSFORM
        print("\n🔄 FASE 3: TRANSFORM (Transformações no Banco)")
        print("-" * 50)
        
        transformation_results = transform_all_years_in_db(db_path)
        
        # Criar views finais
        create_final_views(db_path)
        
        print("✅ Transform ELT concluído com sucesso!")
        
        # RESUMO FINAL
        end_time = datetime.now()
        execution_time = end_time - start_time
        
        print("\n" + "=" * 50)
        print("🎯 PIPELINE ELT CONCLUÍDO COM SUCESSO!")
        print("=" * 50)
        
        total_raw_records = sum(load_results.values())
        total_transformed_records = sum(
            result.get('data_types', 0) 
            for result in transformation_results.values() 
            if isinstance(result, dict) and 'error' not in result
        )
        
        print(f"""
📊 RESULTADOS ELT:
   • Registros brutos carregados: {total_raw_records:,}
   • Registros transformados: {total_transformed_records:,}
   • Anos processados: {len(transformation_results)}
   • Banco de dados: {db_path}

🔄 PIPELINE ELT EXECUTADO:
   • EXTRACT: Dados brutos extraídos das URLs
   • LOAD: Dados carregados no banco sem transformação
   • TRANSFORM: Transformações executadas no banco

⏱️ PERFORMANCE:
   • Tempo de execução: {execution_time}
   • Início: {start_time.strftime('%H:%M:%S')}
   • Fim: {end_time.strftime('%H:%M:%S')}

📁 ARTEFATOS GERADOS:
   • Banco de dados: {db_path}
   • Tabelas criadas: itbi_raw, itbi_transformed, itbi_metrics
   • Views: v_itbi_consolidado, v_analise_temporal, v_analise_bairros

✅ STATUS: SUCESSO
        """)
        
        # Mostrar detalhes dos resultados por ano
        print("\n📈 RESULTADOS POR ANO:")
        for year, results in transformation_results.items():
            if isinstance(results, dict) and 'error' not in results:
                print(f"   • {year}:")
                print(f"     - Registros transformados: {results.get('data_types', 0):,}")
                print(f"     - Métricas derivadas: {results.get('derived_metrics', 0):,}")
                print(f"     - Métricas agregadas: {results.get('aggregated_metrics', 0)}")
            else:
                print(f"   • {year}: ❌ Erro - {results.get('error', 'Erro desconhecido')}")
        
        return db_path
        
    except Exception as e:
        print(f"\n❌ ERRO NO PIPELINE ELT: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def show_database_info(db_path: str):
    """
    Mostra informações do banco de dados criado.
    
    Args:
        db_path (str): Caminho do banco
    """
    
    import sqlite3
    import pandas as pd
    
    print(f"\n📊 INFORMAÇÕES DO BANCO DE DADOS: {db_path}")
    print("-" * 50)
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Listar tabelas
        tables_query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        tables = pd.read_sql_query(tables_query, conn)
        
        print("📋 Tabelas criadas:")
        for table in tables['name']:
            count_query = f"SELECT COUNT(*) as count FROM {table}"
            count = pd.read_sql_query(count_query, conn)['count'][0]
            print(f"   • {table}: {count:,} registros")
        
        # Listar views
        views_query = "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        views = pd.read_sql_query(views_query, conn)
        
        if len(views) > 0:
            print("\n👁️ Views criadas:")
            for view in views['name']:
                print(f"   • {view}")
        
        # Mostrar sample dos dados transformados
        sample_query = """
        SELECT source_year, COUNT(*) as registros, 
               AVG(valor_avaliacao) as valor_medio,
               AVG(valor_por_m2) as valor_m2_medio
        FROM itbi_transformed 
        GROUP BY source_year 
        ORDER BY source_year
        """
        
        sample_data = pd.read_sql_query(sample_query, conn)
        
        print("\n📈 Resumo dos dados transformados:")
        for _, row in sample_data.iterrows():
            print(f"   • {row['source_year']}: {row['registros']:,} registros")
            print(f"     - Valor médio: R$ {row['valor_medio']:,.2f}")
            if row['valor_m2_medio']:
                print(f"     - Valor/m² médio: R$ {row['valor_m2_medio']:,.2f}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Criar diretórios necessários
    os.makedirs("datasets", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Executar pipeline ELT
    db_path = main()
    
    # Mostrar informações do banco
    show_database_info(db_path)
    
    print("\n🎉 Pipeline ELT executado com sucesso!")
    print(f"📁 Banco de dados criado em: {db_path}")
    print("\n💡 Próximos passos:")
    print("   1. Executar análises nos dados transformados")
    print("   2. Criar visualizações")
    print("   3. Gerar insights de negócio")