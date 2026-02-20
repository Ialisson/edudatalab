import duckdb
import os

CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_BASE = os.path.abspath(os.path.join(CAMINHO_ATUAL, "..", ".."))

CAMINHO_SILVER = os.path.join(CAMINHO_BASE, "data", "silver", "ideb_limpo.parquet")
CAMINHO_GOLD_DIR = os.path.join(CAMINHO_BASE, "data", "gold")
CAMINHO_GOLD_RESUMO = os.path.join(CAMINHO_GOLD_DIR, "indicadores_municipais.parquet")
CAMINHO_GOLD_HISTORICO = os.path.join(CAMINHO_GOLD_DIR, "historico_ideb.parquet")

def criar_camada_gold():
    os.makedirs(CAMINHO_GOLD_DIR, exist_ok=True)
    
    # 1. Tabela de Capitais
    query_resumo = f"""
        COPY (
            SELECT nome_municipio, COUNT(*) as total_registros, ROUND(AVG(nota_ideb), 2) as media_ideb
            FROM read_parquet('{CAMINHO_SILVER}')
            GROUP BY nome_municipio ORDER BY media_ideb DESC
        ) TO '{CAMINHO_GOLD_RESUMO}' (FORMAT PARQUET);
    """
    duckdb.sql(query_resumo)
    
    # 2. Nova Tabela de Histórico por Ano
    query_historico = f"""
        COPY (
            SELECT ano, ROUND(AVG(nota_ideb), 2) as media_anual
            FROM read_parquet('{CAMINHO_SILVER}')
            GROUP BY ano ORDER BY ano ASC
        ) TO '{CAMINHO_GOLD_HISTORICO}' (FORMAT PARQUET);
    """
    duckdb.sql(query_historico)
    print("✅ Camada Gold gerada com sucesso (Tabelas de Resumo e Histórico)!")

if __name__ == "__main__":
    criar_camada_gold()