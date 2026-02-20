import polars as pl
import os
from datetime import datetime

# 1. Configuração de Caminhos Dinâmicos
CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_BASE = os.path.abspath(os.path.join(CAMINHO_ATUAL, "..", ".."))

# Caminho de entrada (Bronze) e saída (Silver)
CAMINHO_BRONZE = os.path.join(CAMINHO_BASE, "data", "bronze", "ideb_bruto.parquet")
CAMINHO_SILVER = os.path.join(CAMINHO_BASE, "data", "silver")

def ler_dados_bronze(caminho: str) -> pl.DataFrame:
    """Lê os dados brutos da camada Bronze."""
    print(f"📖 Lendo dados da Bronze: {caminho}")
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}. Rode a ingestão bronze primeiro.")
    return pl.read_parquet(caminho)

def limpar_e_padronizar(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica regras de qualidade de dados (Silver):
    - Padroniza strings (caixa baixa, sem espaços extras).
    - Adiciona metadados de rastreabilidade da camada.
    """
    print("🧹 Aplicando regras de qualidade (Camada Silver)...")
    
    df_silver = (
        df
        # Padronização: Tratamento de strings para evitar duplicações lógicas
        .with_columns([
            pl.col("nome_municipio").str.to_lowercase().str.strip_chars().alias("nome_municipio"),
            pl.col("rede").str.to_lowercase().str.strip_chars().alias("rede")
        ])
        # Governança: Novo carimbo temporal para auditoria da camada Silver
        .with_columns(
            pl.lit(datetime.now()).alias("_data_processamento_silver")
        )
    )
    
    return df_silver

def salvar_na_silver(df: pl.DataFrame):
    """Salva os dados limpos e padronizados no formato Lakehouse."""
    os.makedirs(CAMINHO_SILVER, exist_ok=True)
    arquivo_saida = os.path.join(CAMINHO_SILVER, "ideb_limpo.parquet")
    
    df.write_parquet(arquivo_saida)
    
    print(f"✅ Sucesso! Arquivo de alta qualidade salvo em: {arquivo_saida}")
    print("\nPrévia dos dados Limpos (Silver):")
    print(df.head())

if __name__ == "__main__":
    # Orquestração do Pipeline Silver
    df_bruto = ler_dados_bronze(CAMINHO_BRONZE)
    df_limpo = limpar_e_padronizar(df_bruto)
    salvar_na_silver(df_limpo)