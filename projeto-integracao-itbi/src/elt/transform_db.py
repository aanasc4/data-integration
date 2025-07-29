"""
Módulo de Transformação no Banco de Dados - Pipeline ELT
Projeto ITBI Recife 2023-2025
"""

import sqlite3
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

def create_transformed_tables_schema(db_path: str) -> None:
    """
    Cria schema das tabelas transformadas.
    
    Args:
        db_path (str): Caminho do banco de dados
    """
    
    print("🏗️ Criando schema das tabelas transformadas...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Tabela principal transformada
    create_transformed_table = """
    CREATE TABLE IF NOT EXISTS itbi_transformed (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        
        -- Dados transformados e tipados
        valor_avaliacao REAL,
        valor_por_m2 REAL,
        bairro TEXT,
        tipo_imovel TEXT,
        data_transacao DATE,
        ano_transacao INTEGER,
        mes_transacao INTEGER,
        trimestre INTEGER,
        
        logradouro TEXT,
        numero TEXT,
        complemento TEXT,
        cep TEXT,
        
        area_terreno REAL,
        area_construida REAL,
        ano_construcao INTEGER,
        idade_imovel INTEGER,
        fracao_ideal REAL,
        
        valores_financiados_sfh REAL,
        tem_financiamento BOOLEAN,
        
        -- Métricas derivadas
        faixa_valor TEXT,
        categoria_area TEXT,
        periodo_construcao TEXT,
        
        -- Metadados
        source_year TEXT,
        transformation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        pipeline_type TEXT DEFAULT 'ELT'
    );
    """
    
    # Tabela de métricas agregadas
    create_metrics_table = """
    CREATE TABLE IF NOT EXISTS itbi_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metric_name TEXT,
        metric_value REAL,
        metric_group TEXT,
        source_year TEXT,
        calculation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Tabela de transformação log
    create_transform_log = """
    CREATE TABLE IF NOT EXISTS transformation_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transformation_step TEXT,
        source_year TEXT,
        records_processed INTEGER,
        records_created INTEGER,
        execution_time_seconds REAL,
        status TEXT,
        error_message TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    cursor.execute(create_transformed_table)
    cursor.execute(create_metrics_table)
    cursor.execute(create_transform_log)
    
    # Criar índices para performance
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_transformed_year ON itbi_transformed(source_year);",
        "CREATE INDEX IF NOT EXISTS idx_transformed_bairro ON itbi_transformed(bairro);",
        "CREATE INDEX IF NOT EXISTS idx_transformed_valor ON itbi_transformed(valor_avaliacao);",
        "CREATE INDEX IF NOT EXISTS idx_transformed_data ON itbi_transformed(data_transacao);",
        "CREATE INDEX IF NOT EXISTS idx_metrics_name ON itbi_metrics(metric_name);",
        "CREATE INDEX IF NOT EXISTS idx_metrics_year ON itbi_metrics(source_year);"
    ]
    
    for index in indices:
        cursor.execute(index)
    
    conn.commit()
    conn.close()
    
    print("   ✅ Schema das tabelas transformadas criado!")

def transform_data_types_in_db(db_path: str, year: str) -> int:
    """
    Transforma tipos de dados diretamente no banco.
    
    Args:
        db_path (str): Caminho do banco
        year (str): Ano a ser processado
        
    Returns:
        int: Número de registros transformados
    """
    
    print(f"🔄 Transformando tipos de dados para {year} no banco...")
    
    conn = sqlite3.connect(db_path)
    
    start_time = datetime.now()
    
    try:
        # SQL para transformar e inserir dados limpos
        transform_sql = """
        INSERT INTO itbi_transformed (
            valor_avaliacao,
            bairro,
            tipo_imovel,
            data_transacao,
            ano_transacao,
            mes_transacao,
            trimestre,
            logradouro,
            numero,
            complemento,
            cep,
            area_terreno,
            area_construida,
            ano_construcao,
            fracao_ideal,
            valores_financiados_sfh,
            source_year,
            pipeline_type
        )
        SELECT 
            -- Converter valor (remover vírgulas e converter para float)
            CAST(REPLACE(REPLACE(valor_avaliacao, '.', ''), ',', '.') AS REAL) as valor_avaliacao,
            
            -- Limpar texto
            TRIM(bairro) as bairro,
            TRIM(tipo_imovel) as tipo_imovel,
            
            -- Converter data
            DATE(data_transacao) as data_transacao,
            CAST(strftime('%Y', data_transacao) AS INTEGER) as ano_transacao,
            CAST(strftime('%m', data_transacao) AS INTEGER) as mes_transacao,
            CASE 
                WHEN CAST(strftime('%m', data_transacao) AS INTEGER) BETWEEN 1 AND 3 THEN 1
                WHEN CAST(strftime('%m', data_transacao) AS INTEGER) BETWEEN 4 AND 6 THEN 2
                WHEN CAST(strftime('%m', data_transacao) AS INTEGER) BETWEEN 7 AND 9 THEN 3
                ELSE 4
            END as trimestre,
            
            TRIM(logradouro) as logradouro,
            TRIM(numero) as numero,
            COALESCE(NULLIF(TRIM(complemento), ''), NULLIF(TRIM(complemento), 'nan'), 'Sem complemento') as complemento,
            TRIM(cep) as cep,
            
            -- Converter áreas
            CAST(REPLACE(REPLACE(area_terreno, '.', ''), ',', '.') AS REAL) as area_terreno,
            CAST(REPLACE(REPLACE(area_construida, '.', ''), ',', '.') AS REAL) as area_construida,
            
            CAST(ano_construcao AS INTEGER) as ano_construcao,
            CAST(REPLACE(REPLACE(fracao_ideal, '.', ''), ',', '.') AS REAL) as fracao_ideal,
            
            -- Converter valores financiados
            CAST(REPLACE(REPLACE(valores_financiados_sfh, '.', ''), ',', '.') AS REAL) as valores_financiados_sfh,
            
            source_year,
            'ELT' as pipeline_type
            
        FROM itbi_raw 
        WHERE source_year = ? 
            AND valor_avaliacao IS NOT NULL 
            AND valor_avaliacao != ''
            AND valor_avaliacao != 'nan';
        """
        
        # Limpar dados existentes do ano
        conn.execute("DELETE FROM itbi_transformed WHERE source_year = ?", (year,))
        
        # Executar transformação
        cursor = conn.cursor()
        cursor.execute(transform_sql, (year,))
        
        records_transformed = cursor.rowcount
        conn.commit()
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Log da transformação
        log_entry = {
            'transformation_step': 'data_types',
            'source_year': year,
            'records_processed': records_transformed,
            'records_created': records_transformed,
            'execution_time_seconds': execution_time,
            'status': 'success',
            'error_message': None
        }
        
        pd.DataFrame([log_entry]).to_sql('transformation_log', conn, if_exists='append', index=False)
        conn.commit()
        
        print(f"   ✅ {records_transformed:,} registros transformados em {execution_time:.2f}s")
        
        return records_transformed
        
    except Exception as e:
        # Log do erro
        error_log = {
            'transformation_step': 'data_types',
            'source_year': year,
            'records_processed': 0,
            'records_created': 0,
            'execution_time_seconds': 0,
            'status': 'error',
            'error_message': str(e)
        }
        
        pd.DataFrame([error_log]).to_sql('transformation_log', conn, if_exists='append', index=False)
        conn.commit()
        
        print(f"   ❌ Erro transformando {year}: {e}")
        raise
        
    finally:
        conn.close()

def create_derived_metrics_in_db(db_path: str, year: str) -> int:
    """
    Cria métricas derivadas diretamente no banco.
    
    Args:
        db_path (str): Caminho do banco
        year (str): Ano a ser processado
        
    Returns:
        int: Número de registros atualizados
    """
    
    print(f"🧮 Criando métricas derivadas para {year} no banco...")
    
    conn = sqlite3.connect(db_path)
    
    start_time = datetime.now()
    
    try:
        cursor = conn.cursor()
        
        # 1. Calcular valor por m²
        update_valor_m2 = """
        UPDATE itbi_transformed 
        SET valor_por_m2 = ROUND(valor_avaliacao / NULLIF(area_construida, 0), 2)
        WHERE source_year = ? AND area_construida > 0;
        """
        
        cursor.execute(update_valor_m2, (year,))
        print(f"   • valor_por_m2 calculado")
        
        # 2. Calcular idade do imóvel
        update_idade = """
        UPDATE itbi_transformed 
        SET idade_imovel = ano_transacao - ano_construcao
        WHERE source_year = ? AND ano_construcao IS NOT NULL;
        """
        
        cursor.execute(update_idade, (year,))
        print(f"   • idade_imovel calculada")
        
        # 3. Indicador de financiamento
        update_financiamento = """
        UPDATE itbi_transformed 
        SET tem_financiamento = CASE 
            WHEN valores_financiados_sfh > 0 THEN 1 
            ELSE 0 
        END
        WHERE source_year = ?;
        """
        
        cursor.execute(update_financiamento, (year,))
        print(f"   • tem_financiamento calculado")
        
        # 4. Faixa de valor
        update_faixa_valor = """
        UPDATE itbi_transformed 
        SET faixa_valor = CASE 
            WHEN valor_avaliacao <= 200000 THEN 'Baixo (até R$ 200k)'
            WHEN valor_avaliacao <= 500000 THEN 'Médio (R$ 200k-500k)'
            WHEN valor_avaliacao <= 1000000 THEN 'Alto (R$ 500k-1M)'
            ELSE 'Premium (acima R$ 1M)'
        END
        WHERE source_year = ?;
        """
        
        cursor.execute(update_faixa_valor, (year,))
        print(f"   • faixa_valor calculada")
        
        # 5. Categoria de área
        update_categoria_area = """
        UPDATE itbi_transformed 
        SET categoria_area = CASE 
            WHEN area_construida <= 50 THEN 'Pequeno (até 50m²)'
            WHEN area_construida <= 100 THEN 'Médio (50-100m²)'
            WHEN area_construida <= 200 THEN 'Grande (100-200m²)'
            ELSE 'Extra Grande (acima 200m²)'
        END
        WHERE source_year = ? AND area_construida IS NOT NULL;
        """
        
        cursor.execute(update_categoria_area, (year,))
        print(f"   • categoria_area calculada")
        
        # 6. Período de construção
        update_periodo_construcao = """
        UPDATE itbi_transformed 
        SET periodo_construcao = CASE 
            WHEN ano_construcao < 1980 THEN 'Antes de 1980'
            WHEN ano_construcao BETWEEN 1980 AND 1999 THEN '1980-1999'
            WHEN ano_construcao BETWEEN 2000 AND 2009 THEN '2000-2009'
            WHEN ano_construcao BETWEEN 2010 AND 2019 THEN '2010-2019'
            ELSE '2020 ou posterior'
        END
        WHERE source_year = ? AND ano_construcao IS NOT NULL;
        """
        
        cursor.execute(update_periodo_construcao, (year,))
        print(f"   • periodo_construcao calculado")
        
        # Contar registros atualizados
        cursor.execute("SELECT COUNT(*) FROM itbi_transformed WHERE source_year = ?", (year,))
        records_updated = cursor.fetchone()[0]
        
        conn.commit()
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Log da transformação
        log_entry = {
            'transformation_step': 'derived_metrics',
            'source_year': year,
            'records_processed': records_updated,
            'records_created': records_updated,
            'execution_time_seconds': execution_time,
            'status': 'success',
            'error_message': None
        }
        
        pd.DataFrame([log_entry]).to_sql('transformation_log', conn, if_exists='append', index=False)
        conn.commit()
        
        print(f"   ✅ Métricas derivadas criadas para {records_updated:,} registros")
        
        return records_updated
        
    except Exception as e:
        error_log = {
            'transformation_step': 'derived_metrics',
            'source_year': year,
            'records_processed': 0,
            'records_created': 0,
            'execution_time_seconds': 0,
            'status': 'error',
            'error_message': str(e)
        }
        
        pd.DataFrame([error_log]).to_sql('transformation_log', conn, if_exists='append', index=False)
        conn.commit()
        
        print(f"   ❌ Erro criando métricas derivadas {year}: {e}")
        raise
        
    finally:
        conn.close()

def calculate_aggregated_metrics(db_path: str, year: str) -> int:
    """
    Calcula métricas agregadas e armazena na tabela de métricas.
    
    Args:
        db_path (str): Caminho do banco
        year (str): Ano a ser processado
        
    Returns:
        int: Número de métricas calculadas
    """
    
    print(f"📊 Calculando métricas agregadas para {year}...")
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Limpar métricas existentes do ano
        conn.execute("DELETE FROM itbi_metrics WHERE source_year = ?", (year,))
        
        # Métricas a serem calculadas
        metrics_queries = {
            'total_transacoes': f"SELECT COUNT(*) FROM itbi_transformed WHERE source_year = '{year}'",
            'valor_medio': f"SELECT AVG(valor_avaliacao) FROM itbi_transformed WHERE source_year = '{year}'",
            'valor_mediano': f"SELECT AVG(valor_avaliacao) FROM (SELECT valor_avaliacao FROM itbi_transformed WHERE source_year = '{year}' ORDER BY valor_avaliacao LIMIT 2 - (SELECT COUNT(*) FROM itbi_transformed WHERE source_year = '{year}') % 2 OFFSET (SELECT (COUNT(*) - 1) / 2 FROM itbi_transformed WHERE source_year = '{year}'))",
            'valor_maximo': f"SELECT MAX(valor_avaliacao) FROM itbi_transformed WHERE source_year = '{year}'",
            'valor_minimo': f"SELECT MIN(valor_avaliacao) FROM itbi_transformed WHERE source_year = '{year}'",
            'area_media': f"SELECT AVG(area_construida) FROM itbi_transformed WHERE source_year = '{year}' AND area_construida IS NOT NULL",
            'valor_m2_medio': f"SELECT AVG(valor_por_m2) FROM itbi_transformed WHERE source_year = '{year}' AND valor_por_m2 IS NOT NULL",
            'percentual_financiamento': f"SELECT AVG(CAST(tem_financiamento AS REAL)) * 100 FROM itbi_transformed WHERE source_year = '{year}'",
            'bairros_unicos': f"SELECT COUNT(DISTINCT bairro) FROM itbi_transformed WHERE source_year = '{year}'",
            'tipos_imoveis_unicos': f"SELECT COUNT(DISTINCT tipo_imovel) FROM itbi_transformed WHERE source_year = '{year}'"
        }
        
        metrics_data = []
        
        for metric_name, query in metrics_queries.items():
            try:
                result = pd.read_sql_query(query, conn).iloc[0, 0]
                
                metrics_data.append({
                    'metric_name': metric_name,
                    'metric_value': float(result) if result is not None else 0.0,
                    'metric_group': 'general',
                    'source_year': year
                })
                
            except Exception as e:
                print(f"   ⚠️ Erro calculando {metric_name}: {e}")
                continue
        
        # Métricas por bairro (top 5)
        bairro_query = f"""
        SELECT bairro, COUNT(*) as count, AVG(valor_avaliacao) as avg_value
        FROM itbi_transformed 
        WHERE source_year = '{year}'
        GROUP BY bairro 
        ORDER BY count DESC 
        LIMIT 5
        """
        
        bairro_metrics = pd.read_sql_query(bairro_query, conn)
        
        for _, row in bairro_metrics.iterrows():
            metrics_data.extend([
                {
                    'metric_name': f'transacoes_{row["bairro"].lower().replace(" ", "_")}',
                    'metric_value': float(row['count']),
                    'metric_group': 'bairros',
                    'source_year': year
                },
                {
                    'metric_name': f'valor_medio_{row["bairro"].lower().replace(" ", "_")}',
                    'metric_value': float(row['avg_value']),
                    'metric_group': 'bairros',
                    'source_year': year
                }
            ])
        
        # Salvar métricas
        if metrics_data:
            pd.DataFrame(metrics_data).to_sql('itbi_metrics', conn, if_exists='append', index=False)
            conn.commit()
            
            print(f"   ✅ {len(metrics_data)} métricas calculadas e armazenadas")
            
        return len(metrics_data)
        
    except Exception as e:
        print(f"   ❌ Erro calculando métricas agregadas: {e}")
        raise
        
    finally:
        conn.close()

def transform_all_years_in_db(db_path: str) -> Dict[str, Dict[str, int]]:
    """
    Executa todas as transformações para todos os anos no banco.
    
    Args:
        db_path (str): Caminho do banco
        
    Returns:
        Dict[str, Dict[str, int]]: Resultados das transformações por ano
    """
    
    print("\n🔄 EXECUTANDO TRANSFORMAÇÕES NO BANCO - PIPELINE ELT")
    print("-" * 55)
    
    # Criar schema das tabelas transformadas
    create_transformed_tables_schema(db_path)
    
    # Obter anos disponíveis
    conn = sqlite3.connect(db_path)
    years_query = "SELECT DISTINCT source_year FROM itbi_raw ORDER BY source_year"
    available_years = pd.read_sql_query(years_query, conn)['source_year'].tolist()
    conn.close()
    
    print(f"📅 Anos encontrados: {available_years}")
    
    transformation_results = {}
    
    for year in available_years:
        print(f"\n🔄 Processando {year}:")
        
        year_results = {}
        
        try:
            # 1. Transformar tipos de dados
            records_transformed = transform_data_types_in_db(db_path, year)
            year_results['data_types'] = records_transformed
            
            # 2. Criar métricas derivadas
            metrics_created = create_derived_metrics_in_db(db_path, year)
            year_results['derived_metrics'] = metrics_created
            
            # 3. Calcular métricas agregadas
            aggregated_metrics = calculate_aggregated_metrics(db_path, year)
            year_results['aggregated_metrics'] = aggregated_metrics
            
            transformation_results[year] = year_results
            
            print(f"   ✅ {year} processado com sucesso!")
            
        except Exception as e:
            print(f"   ❌ Erro processando {year}: {e}")
            transformation_results[year] = {'error': str(e)}
    
    print(f"\n✅ Transformações ELT concluídas!")
    print(f"   • Anos processados: {len(transformation_results)}")
    
    return transformation_results

def create_final_views(db_path: str) -> None:
    """
    Cria views finais para análise dos dados transformados.
    
    Args:
        db_path (str): Caminho do banco
    """
    
    print("\n📋 Criando views finais para análise...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # View consolidada com todos os dados
        view_consolidated = """
        CREATE VIEW IF NOT EXISTS v_itbi_consolidado AS
        SELECT 
            *,
            CASE 
                WHEN valor_por_m2 < 3000 THEN 'Baixo'
                WHEN valor_por_m2 < 6000 THEN 'Médio'
                ELSE 'Alto'
            END as categoria_valor_m2
        FROM itbi_transformed
        ORDER BY data_transacao DESC;
        """
        
        # View de análise temporal
        view_temporal = """
        CREATE VIEW IF NOT EXISTS v_analise_temporal AS
        SELECT 
            source_year,
            ano_transacao,
            mes_transacao,
            COUNT(*) as total_transacoes,
            AVG(valor_avaliacao) as valor_medio,
            AVG(valor_por_m2) as valor_m2_medio,
            SUM(CASE WHEN tem_financiamento = 1 THEN 1 ELSE 0 END) as com_financiamento
        FROM itbi_transformed
        GROUP BY source_year, ano_transacao, mes_transacao
        ORDER BY source_year, ano_transacao, mes_transacao;
        """
        
        # View de análise por bairro
        view_bairros = """
        CREATE VIEW IF NOT EXISTS v_analise_bairros AS
        SELECT 
            bairro,
            source_year,
            COUNT(*) as total_transacoes,
            AVG(valor_avaliacao) as valor_medio,
            MIN(valor_avaliacao) as valor_minimo,
            MAX(valor_avaliacao) as valor_maximo,
            AVG(valor_por_m2) as valor_m2_medio,
            AVG(area_construida) as area_media
        FROM itbi_transformed
        WHERE bairro IS NOT NULL
        GROUP BY bairro, source_year
        ORDER BY total_transacoes DESC;
        """
        
        cursor.execute(view_consolidated)
        cursor.execute(view_temporal)
        cursor.execute(view_bairros)
        
        conn.commit()
        
        print("   ✅ Views finais criadas:")
        print("      • v_itbi_consolidado: Dados completos com categorias")
        print("      • v_analise_temporal: Análise por tempo")
        print("      • v_analise_bairros: Análise por bairro")
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Teste do módulo
    print("🧪 Testando módulo ELT Transform DB...")
    
    # Este módulo precisa de dados já carregados no banco
    # Para testar, execute primeiro o load_raw.py
    
    test_db = "test_elt_transform.db"
    
    try:
        # Criar schema de teste
        create_transformed_tables_schema(test_db)
        print("✅ Teste de schema concluído!")
        
    finally:
        import os
        if os.path.exists(test_db):
            os.remove(test_db)