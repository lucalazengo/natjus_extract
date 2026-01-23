import ijson
import os
import re
import sys
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

# =========================
# CONFIGURAÇÕES
# =========================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed_data")
INPUT_JSON = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.json")

NOME_INDICE = "vw-natjus_tjgo"

# --- MUDANÇA CRÍTICA: BATCH_SIZE 1 ---
# Para evitar estouro de memória (Circuit Breaking Exception) com PDFs em Base64
BATCH_SIZE = 1 

elastic_url = os.getenv("ELASTICSEARCH_URL")
if not elastic_url:
    print("ERRO: Configure ELASTICSEARCH_URL")
    sys.exit(1)

cliente = Elasticsearch(
    elastic_url,
    basic_auth=(os.getenv("ELASTIC_USERNAME"), os.getenv("ELASTIC_PASSWORD")),
    verify_certs=False,
    ssl_show_warn=False,
    request_timeout=120, # Timeout maior por documento
    retry_on_timeout=True
)

# =========================
# MAPEAMENTO
# =========================

def recriar_indice():
    # Verifica se índice existe antes de deletar
    if cliente.indices.exists(index=NOME_INDICE):
        print(f"⚠️  Deletando índice antigo '{NOME_INDICE}'...")
        cliente.indices.delete(index=NOME_INDICE)
    
    mapping = {
        "mappings": {
            "properties": {
                "is_legado": {"type": "boolean"},
                "source_filename": {"type": "keyword"},
                "processo": {"type": "keyword"},
                "cid": {"type": "keyword"},
                "desfecho": {"type": "keyword"},
                "tipo_arquivo": {"type": "keyword"},
                "inteiro_teor": {"type": "text", "analyzer": "portuguese"},
                "n_nota_tecnica": {"type": "text", "analyzer": "portuguese"},
                "assunto": {"type": "text", "analyzer": "portuguese"},
                "classificacao": {"type": "keyword"},
                "objeto": {"type": "text", "analyzer": "portuguese"},
                "medicamento_e_insumo": {"type": "text", "analyzer": "portuguese"},
                "informacao_complementar": {"type": "text", "analyzer": "portuguese"},
                "classificador_do_objeto": {"type": "text", "analyzer": "portuguese"},
                "data_do_envio": {"type": "date", "format": "yyyy-MM-dd"},
                
                # Binary e Doc Values False para economizar memória
                "conteudo_pdf_base64": {"type": "binary", "doc_values": False}
            }
        }
    }
    
    print(f"🔨 Criando índice '{NOME_INDICE}'...")
    cliente.indices.create(index=NOME_INDICE, body=mapping)

# =========================
# UTILITÁRIOS
# =========================

MESES = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
}

def converter_data(data_extenso):
    if not data_extenso: return None
    try:
        match = re.search(r"(\d{1,2})\s*de\s*([A-Za-zç]+)\s*de\s*(\d{4})", data_extenso, re.IGNORECASE)
        if match:
            dia, mes_nome, ano = match.groups()
            mes_numero = MESES.get(mes_nome.lower())
            if mes_numero: return f"{ano}-{mes_numero}-{dia.zfill(2)}"
    except: pass
    return None

def gerar_acoes(arquivo_json):
    with open(arquivo_json, "rb") as f:
        objetos = ijson.items(f, "item")
        count = 0
        for item in objetos:
            count += 1
            filename = item.get("source_filename", "desconhecido")
            print(f"[{count}] Preparando: {filename}")
            
            data_iso = converter_data(item.get("data_do_envio"))
            legado = item.get("is_legado")
            if isinstance(legado, str): Legacy = (legado.lower() == "true")

            doc = {
                "_index": NOME_INDICE,
                "_source": {
                    "is_legado": legado,
                    "source_filename": filename, # Usa a var local que pegamos acima
                    "tipo_arquivo": item.get("tipo_arquivo"),
                    "processo": item.get("processo"),
                    "cid": item.get("cid"), 
                    "n_nota_tecnica": item.get("n_nota_tecnica"),
                    "desfecho": item.get("desfecho"),
                    "inteiro_teor": item.get("inteiro_teor", ""), 
                    "assunto": item.get("Assunto"),
                    "classificacao": item.get("Classificação"),
                    "objeto": item.get("objeto"),
                    "classificador_do_objeto": item.get("classificador_do_objeto"),
                    "informacao_complementar": item.get("informacao_complementar"),
                    "medicamento_e_insumo": item.get("medicamento_e_insumo"),
                    "conteudo_pdf_base64": item.get("conteudo_pdf_base64"),
                    "data_do_envio": data_iso
                }
            }
            yield doc

def main():
    if not os.path.exists(INPUT_JSON):
        print("JSON não encontrado.")
        return

    recriar_indice()

    print(f"Iniciando indexação unitária (BATCH_SIZE={BATCH_SIZE})...")
    
    # Como estamos usando Batch=1, vamos iterar manualmente para ter controle total de erro
    gerador = gerar_acoes(INPUT_JSON)
    
    sucesso_total = 0
    falhas_total = 0
    
    for doc in gerador:
        try:
            # Envia 1 documento por vez
            cliente.index(index=NOME_INDICE, document=doc["_source"])
            sucesso_total += 1
        except Exception as e:
            falhas_total += 1
            nome = doc["_source"].get("source_filename", "desconhecido")
            print(f"❌ ERRO ao indexar arquivo '{nome}': {e}")
            # Se for erro de memória (429), damos uma pausa para o Elastic respirar
            if "429" in str(e):
                print("⚠️  Memória cheia (Circuit Breaker). Pausando 5s...")
                time.sleep(5)
    
    print("-" * 40)
    print(f"✅ Processo finalizado!")
    print(f"Sucessos: {sucesso_total}")
    print(f"Falhas: {falhas_total}")

if __name__ == "__main__":
    main()