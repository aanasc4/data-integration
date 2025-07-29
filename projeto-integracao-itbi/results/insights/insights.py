"""
Módulo de Análises e Insights
Projeto ITBI Recife 2023-2025
"""

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

class ITBIAnalyzer:
    """
    Classe para realizar análises dos dados ITBI consolidados.
    """
    
    def __init__(self, db_path: str):
        """
        Inicializa o analisador com o caminho do banco de dados.
        
        Args:
            db_path (str): Caminho do banco de dados SQLite
        """
        self.db_path = db_path
        self.df = None
        self._load_data()
        
    def _load_data(self):
        """Carrega dados do banco para análise."""
        
        print("📊 Carregando dados para análise...")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Carregar dados transformados
            query = """
            SELECT * FROM itbi_transformed 
            WHERE valor_avaliacao IS NOT NULL 
            AND valor_avaliacao > 0
            ORDER BY data_transacao DESC
            """
            
            self.df = pd.read_sql_query(query, conn)
            
            # Converter data_transacao para datetime
            self.df['data_transacao'] = pd.to_datetime(self.df['data_transacao'])
            
            print(f"   ✅ {len(self.df):,} registros carregados")
            print(f"   📅 Período: {self.df['source_year'].min()} - {self.df['source_year'].max()}")
            
        finally:
            conn.close()
    
    def analise_temporal(self) -> Dict:
        """
        ANÁLISE 1: Evolução temporal dos valores imobiliários.
        
        Returns:
            Dict: Resultados da análise temporal
        """
        
        print("\n📈 ANÁLISE 1: EVOLUÇÃO TEMPORAL DOS VALORES")
        print("-" * 45)
        
        # Análise por ano
        yearly_analysis = self.df.groupby('source_year').agg({
            'valor_avaliacao': ['count', 'mean', 'median', 'std'],
            'valor_por_m2': 'mean',
            'tem_financiamento': 'mean'
        }).round(2)
        
        yearly_analysis.columns = [
            'total_transacoes', 'valor_medio', 'valor_mediano', 
            'desvio_padrao', 'valor_m2_medio', 'perc_financiamento'
        ]
        
        # Análise mensal
        monthly_analysis = self.df.groupby(['source_year', 'mes_transacao']).agg({
            'valor_avaliacao': ['count', 'mean']
        }).round(2)
        
        monthly_analysis.columns = ['transacoes_mes', 'valor_medio_mes']
        monthly_analysis = monthly_analysis.reset_index()
        
        # Calcular crescimento ano a ano
        "crescimento_valor = []\n",
        "crescimento_transacoes = []\n",
        "\n",
        "years = sorted(yearly_analysis.index)\n",
        "for i in range(1, len(years)):\n",
        "    year_atual = years[i]\n",
        "    year_anterior = years[i-1]\n",
        "    \n",
        "    cresc_valor = ((yearly_analysis.loc[year_atual, 'valor_medio'] - \n",
        "                   yearly_analysis.loc[year_anterior, 'valor_medio']) / \n",
        "                   yearly_analysis.loc[year_anterior, 'valor_medio']) * 100\n",
        "    \n",
        "    cresc_transacoes = ((yearly_analysis.loc[year_atual, 'total_transacoes'] - \n",
        "                        yearly_analysis.loc[year_anterior, 'total_transacoes']) / \n",
        "                        yearly_analysis.loc[year_anterior, 'total_transacoes']) * 100\n",
        "    \n",
        "    crescimento_valor.append((year_atual, cresc_valor))\n",
        "    crescimento_transacoes.append((year_atual, cresc_transacoes))\n",
        "\n",
        "# Visualização\n",
        "fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))\n",
        "fig.suptitle('📈 Análise Temporal - Mercado Imobiliário Recife', fontsize=16, fontweight='bold')\n",
        "\n",
        "# Gráfico 1: Evolução do número de transações\n",
        "yearly_analysis['total_transacoes'].plot(kind='bar', ax=ax1, color='skyblue')\n",
        "ax1.set_title('Evolução do Número de Transações')\n",
        "ax1.set_ylabel('Número de Transações')\n",
        "ax1.tick_params(axis='x', rotation=0)\n",
        "\n",
        "# Gráfico 2: Evolução do valor médio\n",
        "yearly_analysis['valor_medio'].plot(kind='line', ax=ax2, marker='o', color='red', linewidth=2)\n",
        "ax2.set_title('Evolução do Valor Médio dos Imóveis')\n",
        "ax2.set_ylabel('Valor Médio (R$)')\n",
        "ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x/1000:.0f}k'))\n",
        "ax2.grid(True, alpha=0.3)\n",
        "\n",
        "# Gráfico 3: Evolução do valor por m²\n",
        "yearly_analysis['valor_m2_medio'].plot(kind='line', ax=ax3, marker='s', color='green', linewidth=2)\n",
        "ax3.set_title('Evolução do Valor por m²')\n",
        "ax3.set_ylabel('Valor por m² (R$)')\n",
        "ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))\n",
        "ax3.grid(True, alpha=0.3)\n",
        "\n",
        "# Gráfico 4: Percentual de financiamento\n",
        "(yearly_analysis['perc_financiamento'] * 100).plot(kind='bar', ax=ax4, color='orange')\n",
        "ax4.set_title('Percentual de Transações com Financiamento')\n",
        "ax4.set_ylabel('Percentual (%)')\n",
        "ax4.tick_params(axis='x', rotation=0)\n",
        "\n",
        "plt.tight_layout()\n",
        "plt.show()\n",
        "\n",
        "# Insights\n",
        "insights_temporais = {\n",
        "    'periodo_analise': f\"{self.df['source_year'].min()}-{self.df['source_year'].max()}\",\n",
        "    'total_transacoes': int(yearly_analysis['total_transacoes'].sum()),\n",
        "    'valor_medio_geral': float(self.df['valor_avaliacao'].mean()),\n",
        "    'crescimento_valor_periodo': crescimento_valor,\n",
        "    'crescimento_transacoes_periodo': crescimento_transacoes,\n",
        "    'ano_mais_ativo': yearly_analysis['total_transacoes'].idxmax(),\n",
        "    'ano_maior_valor': yearly_analysis['valor_medio'].idxmax(),\n",
        "    'sazonalidade': monthly_analysis.groupby('mes_transacao')['transacoes_mes'].mean().to_dict()\n",
        "}\n",
        "\n",
        "print(\"\\n🔍 INSIGHTS DA ANÁLISE TEMPORAL:\")\n",
        "print(f\"   • Período analisado: {insights_temporais['periodo_analise']}\")\n",
        "print(f\"   • Total de transações: {insights_temporais['total_transacoes']:,}\")\n",
        "print(f\"   • Valor médio geral: R$ {insights_temporais['valor_medio_geral']:,.2f}\")\n",
        "print(f\"   • Ano mais ativo: {insights_temporais['ano_mais_ativo']}\")\n",
        "print(f\"   • Ano com maior valor médio: {insights_temporais['ano_maior_valor']}\")\n",
        "\n",
        "if crescimento_valor:\n",
        "    for year, cresc in crescimento_valor:\n",
        "        print(f\"   • Crescimento {year}: {cresc:+.1f}% no valor médio\")\n",
        "\n",
        "return insights_temporais\n",
        "\n",
        "    def analise_geografica(self) -> Dict:\n",
        "        \"\"\"\n",
        "        ANÁLISE 2: Análise geográfica por bairros.\n",
        "        \n",
        "        Returns:\n",
        "            Dict: Resultados da análise geográfica\n",
        "        \"\"\"\n",
        "        \n",
        "        print(\"\\n🏘️ ANÁLISE 2: ANÁLISE GEOGRÁFICA POR BAIRROS\")\n",
        "        print(\"-\" * 45)\n",
        "        \n",
        "        # Análise por bairros\n",
        "        bairro_analysis = self.df.groupby('bairro').agg({\n",
        "            'valor_avaliacao': ['count', 'mean', 'median', 'std'],\n",
        "            'valor_por_m2': ['mean', 'median'],\n",
        "            'area_construida': 'mean',\n",
        "            'tem_financiamento': 'mean'\n",
        "        }).round(2)\n",
        "        \n",
        "        bairro_analysis.columns = [\n",
        "            'total_transacoes', 'valor_medio', 'valor_mediano', 'desvio_padrao',\n",
        "            'valor_m2_medio', 'valor_m2_mediano', 'area_media', 'perc_financiamento'\n",
        "        ]\n",
        "        \n",
        "        bairro_analysis = bairro_analysis.reset_index().sort_values('total_transacoes', ascending=False)\n",
        "        \n",
        "        # Top 20 bairros\n",
        "        top_bairros = bairro_analysis.head(20)\n",
        "        \n",
        "        # Evolução por bairro ao longo do tempo (top 5)\n",
        "        top_5_bairros = top_bairros.head(5)['bairro'].tolist()\n",
        "        \n",
        "        evolucao_bairros = self.df[self.df['bairro'].isin(top_5_bairros)].groupby(\n",
        "            ['bairro', 'source_year']\n",
        "        )['valor_avaliacao'].mean().unstack(fill_value=0)\n",
        "        \n",
        "        # Visualizações\n",
        "        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))\n",
        "        fig.suptitle('🏘️ Análise Geográfica - Top Bairros Recife', fontsize=16, fontweight='bold')\n",
        "        \n",
        "        # Gráfico 1: Top 10 bairros por número de transações\n",
        "        top_bairros.head(10).plot(x='bairro', y='total_transacoes', kind='barh', ax=ax1, color='steelblue')\n",
        "        ax1.set_title('Top 10 Bairros - Número de Transações')\n",
        "        ax1.set_xlabel('Número de Transações')\n",
        "        \n",
        "        # Gráfico 2: Top 10 bairros por valor médio\n",
        "        top_valor_bairros = bairro_analysis.nlargest(10, 'valor_medio')\n",
        "        top_valor_bairros.plot(x='bairro', y='valor_medio', kind='barh', ax=ax2, color='goldenrod')\n",
        "        ax2.set_title('Top 10 Bairros - Valor Médio')\n",
        "        ax2.set_xlabel('Valor Médio (R$)')\n",
        "        ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x/1000:.0f}k'))\n",
        "        \n",
        "        # Gráfico 3: Evolução dos top 5 bairros\n",
        "        evolucao_bairros.T.plot(kind='line', ax=ax3, marker='o')\n",
        "        ax3.set_title('Evolução do Valor Médio - Top 5 Bairros')\n",
        "        ax3.set_ylabel('Valor Médio (R$)')\n",
        "        ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x/1000:.0f}k'))\n",
        "        ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left')\n",
        "        ax3.grid(True, alpha=0.3)\n",
        "        \n",
        "        # Gráfico 4: Scatter valor vs número de transações\n",
        "        ax4.scatter(top_bairros['total_transacoes'], top_bairros['valor_medio'], \n",
        "                   s=top_bairros['area_media']*2, alpha=0.6, color='purple')\n",
        "        ax4.set_title('Valor Médio vs Número de Transações\\n(Tamanho = Área Média)')\n",
        "        ax4.set_xlabel('Número de Transações')\n",
        "        ax4.set_ylabel('Valor Médio (R$)')\n",
        "        ax4.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x/1000:.0f}k'))\n",
        "        \n",
        "        # Adicionar rótulos aos principais pontos\n",
        "        for i, row in top_bairros.head(5).iterrows():\n",
        "            ax4.annotate(row['bairro'], (row['total_transacoes'], row['valor_medio']), \n",
        "                        xytext=(5, 5), textcoords='offset points', fontsize=8)\n",
        "        \n",
        "        plt.tight_layout()\n",
        "        plt.show()\n",
        "        \n",
        "        # Calcular índices de concentração\n",
        "        total_transacoes = bairro_analysis['total_transacoes'].sum()\n",
        "        concentracao_top5 = top_bairros.head(5)['total_transacoes'].sum() / total_transacoes * 100\n",
        "        concentracao_top10 = top_bairros.head(10)['total_transacoes'].sum() / total_transacoes * 100\n",
        "        \n",
        "        insights_geograficos = {\n",
        "            'total_bairros': len(bairro_analysis),\n",
        "            'bairro_mais_ativo': top_bairros.iloc[0]['bairro'],\n",
        "            'transacoes_bairro_ativo': int(top_bairros.iloc[0]['total_transacoes']),\n",
        "            'bairro_mais_valorizado': top_valor_bairros.iloc[0]['bairro'],\n",
        "            'valor_medio_mais_alto': float(top_valor_bairros.iloc[0]['valor_medio']),\n",
        "            'concentracao_top5': float(concentracao_top5),\n",
        "            'concentracao_top10': float(concentracao_top10),\n",
        "            'top_10_bairros': top_bairros.head(10)[['bairro', 'total_transacoes', 'valor_medio']].to_dict('records')\n",
        "        }\n",
        "        \n",
        "        print(\"\\n🔍 INSIGHTS DA ANÁLISE GEOGRÁFICA:\")\n",
        "        print(f\"   • Total de bairros: {insights_geograficos['total_bairros']}\")\n",
        "        print(f\"   • Bairro mais ativo: {insights_geograficos['bairro_mais_ativo']} ({insights_geograficos['transacoes_bairro_ativo']} transações)\")\n",
        "        print(f\"   • Bairro mais valorizado: {insights_geograficos['bairro_mais_valorizado']} (R$ {insights_geograficos['valor_medio_mais_alto']:,.2f})\")\n",
        "        print(f\"   • Concentração top 5 bairros: {insights_geograficos['concentracao_top5']:.1f}% das transações\")\n",
        "        print(f\"   • Concentração top 10 bairros: {insights_geograficos['concentracao_top10']:.1f}% das transações\")\n",
        "        \n",
        "        return insights_geograficos\n",
        "    \n",
        "    def analise_segmentacao(self) -> Dict:\n",
        "        \"\"\"\n",
        "        ANÁLISE 3: Segmentação por tipo de imóvel e características.\n",
        "        \n",
        "        Returns:\n",
        "            Dict: Resultados da análise de segmentação\n",
        "        \"\"\"\n",
        "        \n",
        "        print(\"\\n🏢 ANÁLISE 3: SEGMENTAÇÃO POR TIPO E CARACTERÍSTICAS\")\n",
        "        print(\"-\" * 55)\n",
        "        \n",
        "        # Análise por tipo de imóvel\n",
        "        tipo_analysis = self.df.groupby('tipo_imovel').agg({\n",
        "            'valor_avaliacao': ['count', 'mean', 'median'],\n",
        "            'valor_por_m2': 'mean',\n",
        "            'area_construida': 'mean',\n",
        "            'tem_financiamento': 'mean',\n",
        "            'idade_imovel': 'mean'\n",
        "        }).round(2)\n",
        "        \n",
        "        tipo_analysis.columns = [\n",
        "            'total_transacoes', 'valor_medio', 'valor_mediano',\n",
        "            'valor_m2_medio', 'area_media', 'perc_financiamento', 'idade_media'\n",
        "        ]\n",
        "        \n",
        "        tipo_analysis = tipo_analysis.reset_index().sort_values('total_transacoes', ascending=False)\n",
        "        \n",
        "        # Análise por faixa de valor\n",
        "        faixa_analysis = self.df.groupby('faixa_valor').agg({\n",
        "            'valor_avaliacao': 'count',\n",
        "            'tem_financiamento': 'mean',\n",
        "            'area_construida': 'mean'\n",
        "        }).round(2)\n",
        "        \n",
        "        faixa_analysis.columns = ['total_transacoes', 'perc_financiamento', 'area_media']\n",
        "        \n",
        "        # Análise por idade do imóvel\n",
        "        idade_bins = [0, 5, 10, 20, 50, 100]\n",
        "        idade_labels = ['0-5 anos', '6-10 anos', '11-20 anos', '21-50 anos', '50+ anos']\n",
        "        \n",
        "        df_idade = self.df.dropna(subset=['idade_imovel'])\n",
        "        df_idade['faixa_idade'] = pd.cut(df_idade['idade_imovel'], bins=idade_bins, labels=idade_labels, right=False)\n",
        "        \n",
        "        idade_analysis = df_idade.groupby('faixa_idade').agg({\n",
        "            'valor_avaliacao': ['count', 'mean'],\n",
        "            'valor_por_m2': 'mean'\n",
        "        }).round(2)\n",
        "        \n",
        "        idade_analysis.columns = ['total_transacoes', 'valor_medio', 'valor_m2_medio']\n",
        "        \n",
        "        # Visualizações\n",
        "        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))\n",
        "        fig.suptitle('🏢 Análise de Segmentação - Tipos e Características', fontsize=16, fontweight='bold')\n",
        "        \n",
        "        # Gráfico 1: Distribuição por tipo de imóvel\n",
        "        top_tipos = tipo_analysis.head(8)\n",
        "        top_tipos.plot(x='tipo_imovel', y='total_transacoes', kind='bar', ax=ax1, color='teal')\n",
        "        ax1.set_title('Distribuição por Tipo de Imóvel')\n",
        "        ax1.set_ylabel('Número de Transações')\n",
        "        ax1.tick_params(axis='x', rotation=45)\n",
        "        \n",
        "        # Gráfico 2: Distribuição por faixa de valor\n",
        "        faixa_analysis['total_transacoes'].plot(kind='pie', ax=ax2, autopct='%1.1f%%', startangle=90)\n",
        "        ax2.set_title('Distribuição por Faixa de Valor')\n",
        "        ax2.set_ylabel('')\n",
        "        \n",
        "        # Gráfico 3: Valor médio por tipo de imóvel\n",
        "        top_tipos.plot(x='tipo_imovel', y='valor_medio', kind='bar', ax=ax3, color='coral')\n",
        "        ax3.set_title('Valor Médio por Tipo de Imóvel')\n",
        "        ax3.set_ylabel('Valor Médio (R$)')\n",
        "        ax3.tick_params(axis='x', rotation=45)\n",
        "        ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x/1000:.0f}k'))\n",
        "        \n",
        "        # Gráfico 4: Valor médio por idade do imóvel\n",
        "        idade_analysis['valor_medio'].plot(kind='bar', ax=ax4, color='lightgreen')\n",
        "        ax4.set_title('Valor Médio por Idade do Imóvel')\n",
        "        ax4.set_ylabel('Valor Médio (R$)')\n",
        "        ax4.tick_params(axis='x', rotation=45)\n",
        "        ax4.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x/1000:.0f}k'))\n",
        "        \n",
        "        plt.tight_layout()\n",
        "        plt.show()\n",
        "        \n",
        "        # Análise de correlação\n",
        "        numeric_cols = ['valor_avaliacao', 'valor_por_m2', 'area_construida', 'idade_imovel']\n",
        "        correlation_matrix = self.df"