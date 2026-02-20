import polars as pl
import numpy as np
import os
from datetime import datetime

CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_BASE = os.path.abspath(os.path.join(CAMINHO_ATUAL, "..", ".."))
CAMINHO_BRONZE_ARQUIVO = os.path.join(CAMINHO_BASE, "data", "bronze", "ideb_bruto.parquet")

def extrair_dados_massivos():
    print("🚀 Iniciando extração MASSIVA e REALISTA de dados...")
    os.makedirs(os.path.dirname(CAMINHO_BRONZE_ARQUIVO), exist_ok=True)
    
    np.random.seed(42)
    num_linhas = 500_000
    
    capitais = [
        "Aracaju", "Belém", "Belo Horizonte", "Boa Vista", "Brasília", 
        "Campo Grande", "Cuiabá", "Curitiba", "Florianópolis", "Fortaleza", 
        "Goiânia", "João Pessoa", "Macapá", "Maceió", "Manaus", "Natal", 
        "Palmas", "Porto Alegre", "Porto Velho", "Recife", "Rio Branco", 
        "Rio de Janeiro", "Salvador", "São Luís", "São Paulo", "Teresina", "Vitória"
    ]
    
    anos = np.random.choice([2015, 2017, 2019, 2021, 2023], num_linhas)
    nomes_municipios = np.random.choice(capitais, num_linhas)
    id_municipios = [abs(hash(nome)) % 1000000 for nome in nomes_municipios]
    redes = np.random.choice(['Pública', 'Privada', 'Municipal', 'Estadual', 'Sem_Informacao'], num_linhas, p=[0.4, 0.2, 0.2, 0.15, 0.05])
    
    # 🧠 INTELIGÊNCIA DE DADOS: Simulando a realidade da educação
    # 1. Cada cidade tem uma base diferente (entre 4.5 e 6.5)
    base_scores = {cap: np.random.uniform(4.8, 6.3) for cap in capitais}
    
    # 2. Modificadores Históricos: Evolução lenta, Queda na Pandemia (2021), Recuperação (2023)
    year_mods = {2015: -0.4, 2017: -0.2, 2019: 0.1, 2021: -0.9, 2023: 0.2}
    
    # Gerando as notas com base na cidade + ano + ruído aleatório das escolas
    notas = np.zeros(num_linhas)
    for i in range(num_linhas):
        cidade_base = base_scores[nomes_municipios[i]]
        mod_ano = year_mods[anos[i]]
        ruido_escola = np.random.normal(0, 0.8) # Variação entre escolas da mesma cidade
        notas[i] = cidade_base + mod_ano + ruido_escola
    
    # Mantendo os erros para a Silver limpar
    notas[np.random.choice(num_linhas, 5000)] = -2.0 
    notas[np.random.choice(num_linhas, 5000)] = 15.5  
    
    df_bruto = pl.DataFrame({
        "ano": anos, "id_municipio": id_municipios,
        "nome_municipio": nomes_municipios, "rede": redes, "nota_ideb": notas
    })
    
    df_bruto = df_bruto.with_columns([
        pl.lit(datetime.now()).alias("_data_ingestao"),
        pl.lit("gerador_massivo_python").alias("_fonte_dados")
    ])
    
    df_bruto.write_parquet(CAMINHO_BRONZE_ARQUIVO)
    print(f"✅ Arquivo gerado com sucesso!")

if __name__ == "__main__":
    extrair_dados_massivos()