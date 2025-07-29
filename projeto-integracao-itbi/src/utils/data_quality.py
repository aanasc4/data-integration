"""
Utilitários para Qualidade de Dados
Projeto ITBI Recife 2023-2025
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple

# Log global de operações
operation_log = []

def log_operation(operation: str, dataset: str, records: int, status: str):
    """
    Registra operação no log.
    
    Args:
        operation (str): Tipo de operação (extract, transform, load)
        dataset (str): Nome do dataset
        records (int): Número de registros processados
        status (str): Status da operação
    """
    
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'operation': operation,
        'dataset': dataset,
        'records': records,
        'status': status
    }
    
    operation_log.append(log_entry)

def validate_itbi_structure(df: pd.DataFrame) -> Dict[str, bool]:
    """
    Valida se o DataFrame tem estrutura típica de dados ITBI.
    
    Args:
        df (pd.DataFrame): DataFrame a ser validado
        
    Returns:
        Dict[str, bool]: Resultados da validação
    """
    
    required_columns = [
        'valor_avaliacao', 'bairro', 'tipo_imovel', 'data_transacao',
        'area_construida', 'logradouro'
    ]
    
    validation_results = {}
    
    # Verificar colunas obrigatórias
    for col in required_columns:
        validation_results[f'has_{col}'] = col in df.columns
    
    # Verificar tipos de dados
    if 'valor_avaliacao' in df.columns:
        validation_results['valor_is_numeric'] = pd.api.types.is_numeric_dtype(df['valor_avaliacao'])
    
    if 'data_transacao' in df.columns:
        validation_results['data_is_datetime'] = pd.api.types.is_datetime64_any_dtype(df['data_transacao'])
    
    # Verificar se não está vazio
    validation_results['not_empty'] = not df.empty
    
    # Verificar duplicatas
    validation_results['no_full_duplicates'] = not df.duplicated().any()
    
    return validation_results

def check_data_quality(df: pd.DataFrame) -> Dict:
    """
    Análise completa de qualidade dos dados.
    
    Args:
        df (pd.DataFrame): DataFrame a ser analisado
        
    Returns:
        Dict: Relatório de qualidade
    """
    
    quality_report = {
        'timestamp': datetime.now().isoformat(),
        'total_records': len(df),
        'total_columns': len(df.columns),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2
    }
    
    # Análise de valores nulos
    null_analysis = {}
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_percentage = (null_count / len(df)) * 100
        null_analysis[col] = {
            'null_count': int(null_count),
            'null_percentage': round(null_percentage, 2)
        }
    
    quality_report['null_analysis'] = null_analysis
    
    # Análise de tipos de dados
    dtype_analysis = {}
    for col in df.columns:
        dtype_analysis[col] = str(df[col].dtype)
    
    quality_report['data_types'] = dtype_analysis
    
    # Análise de duplicatas
    quality_report['duplicates'] = {
        'total_duplicates': int(df.duplicated().sum()),
        'duplicate_percentage': round((df.duplicated().sum() / len(df)) * 100, 2)
    }
    
    # Análise de completude
    total_cells = len(df) * len(df.columns)
    null_cells = df.isnull().sum().sum()
    completeness = ((total_cells - null_cells) / total_cells) * 100
    
    quality_report['completeness'] = {
        'total_cells': int(total_cells),
        'null_cells': int(null_cells),
        'completeness_percentage': round(completeness, 2)
    }
    
    # Validação específica ITBI
    itbi_validation = validate_itbi_structure(df)
    quality_report['itbi_validation'] = itbi_validation
    
    return quality_report

def generate_quality_report(df: pd.DataFrame, output_dir: str = "results/quality") -> str:
    """
    Gera relatório completo de qualidade e salva em arquivo.
    
    Args:
        df (pd.DataFrame): DataFrame a ser analisado
        output_dir (str): Diretório para salvar o relatório
        
    Returns:
        str: Caminho do arquivo do relatório
    """
    
    print("\n📊 GERANDO RELATÓRIO DE QUALIDADE")
    print("-" * 35)
    
    # Criar diretório se necessário
    os.makedirs(output_dir, exist_ok=True)
    
    # Gerar análise de qualidade
    quality_report = check_data_quality(df)
    
    # Adicionar log de operações
    quality_report['operation_log'] = operation_log
    
    # Salvar como JSON
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_path = os.path.join(output_dir, f'quality_report_{timestamp}.json')
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(quality_report, f, indent=2, ensure_ascii=False)
    
    # Gerar relatório em texto legível
    txt_path = os.path.join(output_dir, f'quality_report_{timestamp}.txt')
    
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write("RELATÓRIO DE QUALIDADE DE DADOS - ITBI RECIFE\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"Data/Hora: {quality_report['timestamp']}\n")
        f.write(f"Total de Registros: {quality_report['total_records']:,}\n")
        f.write(f"Total de Colunas: {quality_report['total_columns']}\n")
        f.write(f"Uso de Memória: {quality_report['memory_usage_mb']:.2f} MB\n\n")
        
        # Completude geral
        completeness = quality_report['completeness']
        f.write("COMPLETUDE DOS DADOS:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Completude Geral: {completeness['completeness_percentage']:.1f}%\n")
        f.write(f"Células Nulas: {completeness['null_cells']:,}\n")
        f.write(f"Total de Células: {completeness['total_cells']:,}\n\n")
        
        # Duplicatas
        duplicates = quality_report['duplicates']
        f.write("ANÁLISE DE DUPLICATAS:\n")
        f.write("-" * 22 + "\n")
        f.write(f"Registros Duplicados: {duplicates['total_duplicates']:,}\n")
        f.write(f"Percentual: {duplicates['duplicate_percentage']:.2f}%\n\n")
        
        # Validação ITBI
        f.write("VALIDAÇÃO ESTRUTURA ITBI:\n")
        f.write("-" * 25 + "\n")
        itbi_val = quality_report['itbi_validation']
        for check, result in itbi_val.items():
            status = "✅" if result else "❌"
            f.write(f"{status} {check}: {result}\n")
        f.write("\n")
        
        # Top 10 colunas com mais nulos
        f.write("TOP 10 COLUNAS COM MAIS NULOS:\n")
        f.write("-" * 30 + "\n")
        null_analysis = quality_report['null_analysis']
        sorted_nulls = sorted(null_analysis.items(), 
                            key=lambda x: x[1]['null_count'], 
                            reverse=True)[:10]
        
        for col, null_info in sorted_nulls:
            if null_info['null_count'] > 0:
                f.write(f"{col}: {null_info['null_count']:,} ({null_info['null_percentage']:.1f}%)\n")
    
    print(f"   📄 Relatório JSON: {json_path}")
    print(f"   📄 Relatório TXT: {txt_path}")
    
    # Imprimir resumo na tela
    print(f"\n📋 RESUMO DE QUALIDADE:")
    print(f"   • Completude: {quality_report['completeness']['completeness_percentage']:.1f}%")
    print(f"   • Duplicatas: {quality_report['duplicates']['total_duplicates']:,}")
    print(f"   • Validação ITBI: {sum(quality_report['itbi_validation'].values())}/{len(quality_report['itbi_validation'])} checks")
    
    return json_path

def validate_bairros_recife(df: pd.DataFrame) -> Dict:
    """
    Valida se os bairros são realmente do Recife.
    
    Args:
        df (pd.DataFrame): DataFrame com coluna 'bairro'
        
    Returns:
        Dict: Resultado da validação
    """
    
    # Lista de bairros conhecidos do Recife
    bairros_recife = [
        'Boa Viagem', 'Várzea', 'Imbiribeira', 'Pina', 'Casa Amarela',
        'Madalena', 'Graças', 'Boa Vista', 'Encruzilhada', 'Torre',
        'Recife', 'Espinheiro', 'Aflitos', 'Derby', 'Rosarinho',
        'Santana', 'São José', 'Apipucos', 'Casa Forte', 'Parnamirim',
        'Hipódromo', 'Tamarineira', 'Jaqueira', 'Monteiro', 'Cordeiro',
        'Ilha do Leite', 'Paissandu', 'Santo Antônio', 'São Geraldo',
        'Bongi', 'Mustardinha', 'Tejipió', 'Totó', 'Curado', 'Barro',
        'Jardim São Paulo', 'Sancho', 'Caxangá', 'Cidade Universitária',
        'Iputinga', 'Torrões', 'Praça da Bandeira', 'Campina do Barreto',
        'Porto da Madeira', 'Cabanga', 'Coelhos', 'Ilha Joana Bezerra',
        'Brasília Teimosa', 'Coque', 'Ilha do Retiro', 'Estância',
        'Afogados', 'Bonança', 'Mangueira', 'Macaxeira', 'Nova Descoberta',
        'Brejo da Guabiraba', 'Córrego do Jenipapo', 'Guabiraba',
        'Passarinho', 'Dois Unidos', 'Alto do Mandu', 'Beberibe',
        'Linha do Tiro', 'Fundão', 'Água Fria', 'Alto Santa Terezinha',
        'Bomba do Hemetério', 'Cajueiro', 'Carepa', 'Cosme e Damião',
        'Delta', 'Encantado', 'Peixinhos', 'Roda de Fogo', 'Torreão',
        'Arruda', 'Campina do Barreto', 'Campo Grande', 'Engenho do Meio',
        'Fairyland', 'Hunting', 'Ipsep', 'Jardim Atlântico', 'Jordão',
        'Presidente Kennedy', 'Setúbal', 'Cohab', 'Ibura', 'Jordão Baixo',
        'Barra de Jangada', 'Candeias', 'Piedade', 'Pocinho', 'Vila Rica',
        'Curado', 'Jardim Uchôa', 'Restinga', 'Rio Doce', 'San Martin',
        'Vasco da Gama', 'Jardim Monte Verde', 'Caçote', 'Moreno'
    ]
    
    if 'bairro' not in df.columns:
        return {'error': 'Coluna bairro não encontrada'}
    
    bairros_encontrados = df['bairro'].value_counts()
    
    # Verificar quantos bairros são reconhecidos
    bairros_validos = 0
    bairros_suspeitos = []
    
    for bairro in bairros_encontrados.index:
        if bairro in bairros_recife:
            bairros_validos += 1
        else:
            bairros_suspeitos.append(bairro)
    
    total_bairros = len(bairros_encontrados)
    percentual_validos = (bairros_validos / total_bairros) * 100
    
    return {
        'total_bairros': total_bairros,
        'bairros_validos': bairros_validos,
        'bairros_suspeitos': bairros_suspeitos[:10],  # Top 10 suspeitos
        'percentual_validos': round(percentual_validos, 1),
        'validacao_ok': percentual_validos >= 80  # 80% de bairros válidos
    }

if __name__ == "__main__":
    # Teste dos utilitários
    print("🧪 Testando utilitários de qualidade...")
    
    # Criar dados de teste
    test_data = pd.DataFrame({
        'valor_avaliacao': [100000.0, 200000.0, None],
        'bairro': ['Boa Viagem', 'Várzea', 'Pina'],
        'tipo_imovel': ['Apartamento', 'Casa', 'Apartamento'],
        'data_transacao': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-03-01']),
        'area_construida': [50.5, 75.0, 60.0],
        'logradouro': ['Rua A', 'Rua B', 'Rua C']
    })
    
    # Testar validação
    validation = validate_itbi_structure(test_data)
    print(f"Validação ITBI: {validation}")
    
    # Testar validação de bairros
    bairros_validation = validate_bairros_recife(test_data)
    print(f"Validação bairros: {bairros_validation}")
    
    print("✅ Testes concluídos!")