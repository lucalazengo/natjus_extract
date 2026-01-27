import json
import os
import sys
from elasticsearch import Elasticsearch, helpers

# =========================
# CONFIGURAÇÕES
# =========================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed_data")
FILE_JSON_ENTRADA = os.path.join(PROCESSED_DATA_DIR, "metadados_com_url.json")

# Conexão (Mantendo o ajuste do IP para Windows)
ELASTIC_HOST = "http://127.0.0.1:9200"
INDEX_NAME = "natjus_notas"

# Dicionário para traduzir os meses
MESES_PT = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
}

# =========================
# MAPPING
# =========================
mapping_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "is_legado": {"type": "boolean"},
            "source_filename": {"type": "keyword"},
            "processo": {"type": "keyword"},
            "cid": {"type": "keyword"},
            "desfecho": {"type": "keyword"},
            "tipo_arquivo": {"type": "keyword"},
            "classificacao": {"type": "keyword"},
            "inteiro_teor": {"type": "text", "analyzer": "portuguese"},
            "n_nota_tecnica": {"type": "text", "analyzer": "portuguese"},
            "assunto": {"type": "text", "analyzer": "portuguese"},
            "objeto": {"type": "text", "analyzer": "portuguese"},
            "medicamento_e_insumo": {"type": "text", "analyzer": "portuguese"},
            "informacao_complementar": {"type": "text", "analyzer": "portuguese"},
            "classificador_do_objeto": {"type": "text", "analyzer": "portuguese"},
            
            # AGORA A DATA VAI ENTRAR CORRETA
            "data_do_envio": {
                "type": "date", 
                "format": "yyyy-MM-dd"
            },
            
            "url_pdf": {"type": "keyword", "index": False},      
            "caminho_arquivo": {"type": "keyword", "index": False} 
        }
    }
}

def converter_data(data_str):
    """
    Converte '2 de dezembro de 2024' para '2024-12-02'
    """
    if not data_str or not isinstance(data_str, str):
        return None
    
    try:
        # Ex: "2 de dezembro de 2024" -> ["2", "de", "dezembro", "de", "2024"]
        partes = data_str.lower().split()
        
        if len(partes) < 5:
            return None # Formato inesperado
            
        dia = partes[0].zfill(2) # Garante "02" em vez de "2"
        mes_nome = partes[2]
        ano = partes[4]
        
        mes_num = MESES_PT.get(mes_nome)
        
        if mes_num:
            return f"{ano}-{mes_num}-{dia}" # Retorna YYYY-MM-DD
        return None
        
    except Exception:
        return None

def conectar_elastic():
    print(f"--- Tentando conectar ao Elastic em {ELASTIC_HOST} ---")
    try:
        es = Elasticsearch(ELASTIC_HOST, request_timeout=30)
        info = es.info()
        print(f"✅ Conectado! Versão: {info['version']['number']}")
        return es
    except Exception as e:
        print(f"❌ FALHA DE CONEXÃO: {e}")
        return None

def recriar_indice(es):
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
    es.indices.create(index=INDEX_NAME, body=mapping_body)
    print(f"Índice '{INDEX_NAME}' recriado.")

def gerar_docs(dados):
    for item in dados:
        # --- AQUI ESTÁ A MÁGICA ---
        # Converte a data antes de indexar
        data_original = item.get("data_do_envio")
        data_formatada = converter_data(data_original)
        
        # Atualiza o item com a data certa (ou None se falhou)
        item["data_do_envio"] = data_formatada
        # --------------------------

        yield {
            "_index": INDEX_NAME,
            "_source": item
        }

def indexar_dados():
    if not os.path.exists(FILE_JSON_ENTRADA):
        print("Arquivo JSON não encontrado.")
        return

    es = conectar_elastic()
    if not es: return

    recriar_indice(es)

    print("Carregando JSON e convertendo datas...")
    with open(FILE_JSON_ENTRADA, "r", encoding="utf-8") as f:
        data = json.load(f)

    try:
        sucesso, falhas = helpers.bulk(es, gerar_docs(data), stats_only=True, chunk_size=500)
        print(f"\n--- SUCESSO! ---")
        print(f"Documentos indexados: {sucesso}")
        print(f"Falhas: {falhas}")
            
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    indexar_dados()