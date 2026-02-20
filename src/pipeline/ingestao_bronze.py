import polars as pl
import os
from datetime import datetime

# 1. Configuração de Caminhos Dinâmicos
CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_BASE = os.path.abspath(os.path.join(CAMINHO_ATUAL, "..", ".."))
CAMINHO_BRONZE = os.path.join(CAMINHO_BASE, "data", "bronze")

def extrair_dados_simulados() -> pl.DataFrame:
    """Simula a extração de dados públicos do INEP (ex: IDEB)"""
    print("📥 Extraindo dados brutos (Simulação INEP/IDEB)...")
    
    dados = {
        "ano": [2019, 2019, 2021, 2021, 2023, 2023],
        "id_municipio": [3550308, 3304557, 3550308, 3304557, 3550308, 3304557],
        "nome_municipio": ["São Paulo", "Rio de Janeiro", "São Paulo", "Rio de Janeiro", "São Paulo", "Rio de Janeiro"],
        "rede": ["Pública", "Pública", "Pública", "Pública", "Pública", "Pública"],
        "nota_ideb": [6.0, 5.7, 5.9, 5.5, 6.2, 5.8]
    }
    return pl.DataFrame(dados)

def salvar_na_bronze(df: pl.DataFrame):
    """Adiciona metadados de governança e salva no formato Lakehouse (Parquet)"""
    print("🛡️ Adicionando metadados de governança...")
    
    # 2. Governança: Adicionando linhagem (Lineage)
    df_bronze = df.with_columns([
        pl.lit(datetime.now()).alias("_data_ingestao"),
        pl.lit("api_inep_simulada").alias("_fonte_dados")
    ])
    
    # 3. Garantir que a pasta da camada Bronze existe
    os.makedirs(CAMINHO_BRONZE, exist_ok=True)
    
    # 4. Salvar em formato colunar de alta performance (Parquet)
    arquivo_saida = os.path.join(CAMINHO_BRONZE, "ideb_bruto.parquet")
    df_bronze.write_parquet(arquivo_saida)
    
    print(f"✅ Sucesso! Arquivo salvo na camada Bronze: {arquivo_saida}")
    print("\nPrévia dos dados:")
    print(df_bronze.head())

if __name__ == "__main__":
    # Execução do pipeline
    df_extraido = extrair_dados_simulados()
    salvar_na_bronze(df_extraido)