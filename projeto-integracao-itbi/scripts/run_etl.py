#!/usr/bin/env python3
"""
Script principal para executar o pipeline ETL completo
Projeto ITBI Recife 2023-2025
"""

import sys
import os
from datetime import datetime

# Adicionar src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.etl.extract import extract_itbi_data, validate_extracted_data
from src.etl.transform import transform_all_datasets
from src.etl.load import consolidate_datasets, save_consolidated_data
from src.utils.data_quality import generate_quality_report

def main():
    """Executa o pipeline ETL completo."""
    
    start_time = datetime.now()
    
    print("🚀 PIPELINE ETL - PROJETO ITBI RECIFE")
    print("=" * 50)
    print(f"Início: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    try:
        # FASE 1: EXTRACT
        print("\n📥 FASE 1: EXTRACT")
        print("-" * 25)
        
        datasets_raw = extract_itbi_data()
        
        if not validate_extracted_data(datasets_raw):
            raise ValueError("Dados extraídos falharam na validação")
        
        print("✅ Extract concluído com sucesso!")
        
        # FASE 2: TRANSFORM  
        print("\n🔄 FASE 2: TRANSFORM")
        print("-" * 25)
        
        datasets_clean = transform_all_datasets(datasets_raw)
        
        print("✅ Transform concluído com sucesso!")
        
        # FASE 3: LOAD
        print("\n💾 FASE 3: LOAD")
        print("-" * 25)
        
        df_consolidated = consolidate_datasets(datasets_clean)
        
        # Salvar dados consolidados
        output_path = save_consolidated_data(df_consolidated, "results/datasets")
        
        print("✅ Load concluído com sucesso!")
        
        # FASE 4: QUALITY REPORT
        print("\n📊 FASE 4: RELATÓRIO DE QUALIDADE")
        print("-" * 35)
        
        quality_report = generate_quality_report(df_consolidated)
        
        # RESUMO FINAL
        end_time = datetime.now()
        execution_time = end_time - start_time
        
        print("\n" + "=" * 50)
        print("🎯 PIPELINE ETL CONCLUÍDO COM SUCESSO!")
        print("=" * 50)
        
        print(f"""
📊 RESULTADOS:
   • Registros processados: {len(df_consolidated):,}
   • Colunas finais: {len(df_consolidated.columns)}
   • Período: {df_consolidated['source_year'].min()}-{df_consolidated['source_year'].max()}
   • Arquivo salvo: {output_path}

⏱️ PERFORMANCE:
   • Tempo de execução: {execution_time}
   • Início: {start_time.strftime('%H:%M:%S')}
   • Fim: {end_time.strftime('%H:%M:%S')}

✅ STATUS: SUCESSO
        """)
        
        return df_consolidated
        
    except Exception as e:
        print(f"\n❌ ERRO NO PIPELINE ETL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # Criar diretórios necessários
    os.makedirs("results/datasets", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Executar pipeline
    result = main()
    
    print("\n🎉 Pipeline ETL executado com sucesso!")
    print("📁 Verifique os arquivos em results/datasets/")