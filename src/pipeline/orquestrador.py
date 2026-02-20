import subprocess
import os
import sys

CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))

def orquestrar_pipeline():
    print("Iniciando o Orquestrador...")
    
    # sys.executable garante que ele use o Python do (.venv) e não o do Windows
    subprocess.run([sys.executable, os.path.join(CAMINHO_ATUAL, "ingestao_bronze.py")])
    subprocess.run([sys.executable, os.path.join(CAMINHO_ATUAL, "processamento_silver.py")])
    subprocess.run([sys.executable, os.path.join(CAMINHO_ATUAL, "transformacao_gold.py")])
    
    print("Pipeline concluído com sucesso!")

if __name__ == "__main__":
    orquestrar_pipeline()