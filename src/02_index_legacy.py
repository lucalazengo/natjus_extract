import json
import os
import re
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# =========================
# CONFIGURAÇÕES DINÂMICAS
# =========================

# 1. Identifica onde este script está salvo no computador atual
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Constrói o caminho para a pasta de dados de forma relativa
# Ajuste aqui se o seu script estiver dentro de uma subpasta (ex: src/scripts)
# Se estiver na raiz do projeto (natjus_extract), isso funcionará perfeitamente.
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed_data")
INPUT_JSON = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.json")

NOME_INDICE = "vw-natjus_tjgo"
BATCH_SIZE = 1000

# =========================
# CONEXÃO ELASTICSEARCH
# =========================

# Verifica variáveis de ambiente antes de tentar conectar
elastic_url = os.getenv("ELASTICSEARCH_URL")
if not elastic_url:
    print("ERRO: Variável de ambiente ELASTICSEARCH_URL não definida.")
    sys.exit(1)

cliente = Elasticsearch(
    elastic_url,
    basic_auth=(os.getenv("ELASTIC_USERNAME"), os.getenv("ELASTIC_PASSWORD")),
    verify_certs=False,
    ssl_show_warn=False,
    request_timeout=60,
    retry_on_timeout=True
)

# =========================
# UTILITÁRIOS
# =========================

MESES = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
}

def converter_data(data_extenso):
    """
    Converte '10 de Janeiro de 2025' para '2025-01-10'.
    Retorna None se falhar.
    """
    if not data_extenso:
        return None
    
    try:
        # Regex flexível para dia, mês e ano
        match = re.search(r"(\d{1,2})\s*de\s*([A-Za-zç]+)\s*de\s*(\d{4})", data_extenso, re.IGNORECASE)
        if match:
            dia, mes_nome, ano = match.groups()
            mes_numero = MESES.get(mes_nome.lower())
            
            if mes_numero:
                return f"{ano}-{mes_numero}-{dia.zfill(2)}"
    except Exception:
        pass
    
    return None

# =========================
# PROCESSO DE INDEXAÇÃO
# =========================

def gerar_acoes(dados):
    """
    Gerador para a API Bulk do Elasticsearch.
    """
    for item in dados:
        data_iso = converter_data(item.get("data_do_envio"))

        doc = {
            "_index": NOME_INDICE,
            "_source": {
                "tipo": item.get("tipo", "legado"), 
                "source_filename": item.get("source_filename"),
                "tipo_arquivo": item.get("tipo_arquivo"),
                "processo": item.get("processo"),
                "cid": item.get("cid"), 
                "n_nota_tecnica": item.get("n_nota_tecnica"),
                "desfecho": item.get("desfecho"),
                "inteiro_teor": item.get("inteiro_teor", ""),
                
                "objeto": item.get("objeto"),
                "classificador_do_objeto": item.get("classificador_do_objeto"),
                "informacao_complementar": item.get("informacao_complementar"),
                "medicamento_e_insumo": item.get("medicamento_e_insumo"),
                
                "data_do_envio": data_iso
            }
        }
        yield doc

def main():
    # Debug: Mostra onde o script está procurando o arquivo
    print(f"Diretório base detectado: {BASE_DIR}")
    print(f"Procurando JSON em: {INPUT_JSON}")

    if not os.path.exists(INPUT_JSON):
        print("\n[ERRO CRÍTICO] Arquivo JSON não encontrado.")
        print(f"Certifique-se de que a pasta 'data' está junto deste script.")
        print(f"Caminho tentado: {INPUT_JSON}")
        return

    print("Carregando arquivo JSON...")
    try:
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            dados = json.load(f)
    except Exception as e:
        print(f"Erro ao ler o arquivo JSON: {e}")
        return

    total_docs = len(dados)
    print(f"Documentos carregados: {total_docs}")
    print(f"Iniciando envio para o Elasticsearch (Índice: {NOME_INDICE})...")

    try:
        sucesso, falhas = bulk(
            cliente, 
            gerar_acoes(dados), 
            chunk_size=BATCH_SIZE,
            stats_only=True,
            raise_on_error=False
        )

        print("-" * 40)
        print(f"✅ Processo finalizado!")
        print(f"Arquivos indexados: {sucesso}")
        print(f"Falhas: {falhas}")
        print("-" * 40)

    except Exception as e:
        print(f"Erro de conexão ou envio: {e}")

if __name__ == "__main__":
    main()