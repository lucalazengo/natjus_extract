import json
import os
from minio import Minio
from minio.error import S3Error

# =========================
# CONFIGURAÇÕES
# =========================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Caminhos de arquivos
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed_data")
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw_data", "NT e PARECERES")
FILE_JSON_ENTRADA = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.json")
FILE_JSON_SAIDA = os.path.join(PROCESSED_DATA_DIR, "metadados_com_url.json")

# Configurações MinIO 
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio@natjus")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "natjus-legado")
SECURE = False

# Lista de campos permitidos no JSON fina
CAMPOS_FINAL_JSON = [
    "source_filename",      
    "tipo_arquivo",
    "processo",
    "Classificação",
    "Assunto",
    "cid",
    "n_nota_tecnica",
    "desfecho",
    "objeto",
    "classificador_do_objeto",
    "informacao_complementar",
    "data_do_envio",
    "medicamento_e_insumo",
    "inteiro_teor",
    "is_legado",
    "url_pdf",
    "caminho_arquivo"
]

def setup_minio():
    """Conecta e cria o bucket se necessário"""
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=SECURE
        )
        
        # Verifica se o bucket existe
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            print(f"Bucket '{MINIO_BUCKET}' criado com sucesso.")
        else:
            print(f"Bucket '{MINIO_BUCKET}' encontrado.")
            
        return client
    except Exception as e:
        print(f"Erro ao conectar no MinIO: {e}")
        return None

def upload_arquivo(client, filename):
    """Envia o arquivo para o MinIO e retorna a URL"""
    if not filename: 
        return None
    
    caminho_local = os.path.join(RAW_DATA_DIR, filename)
    
    if not os.path.exists(caminho_local):
        print(f" [AVISO] Arquivo local não encontrado: {filename}")
        return None

    try:
        # Upload do arquivo
        client.fput_object(
            MINIO_BUCKET, 
            filename, 
            caminho_local, 
            content_type="application/pdf"
        )
        
        # Gera a URL
        protocolo = "https" if SECURE else "http"
        url = f"{protocolo}://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
        return url

    except S3Error as e:
        print(f" [ERRO MINIO] Falha ao enviar {filename}: {e}")
        return None
    except Exception as e:
        print(f" [ERRO GERAL] {e}")
        return None

def processar_arquivos():
    if not os.path.exists(FILE_JSON_ENTRADA):
        print(f"Arquivo de entrada não encontrado: {FILE_JSON_ENTRADA}")
        return

    print(f"\n--- Iniciando Upload para MinIO ({MINIO_ENDPOINT}) ---")
    client = setup_minio()
    if not client:
        return

    with open(FILE_JSON_ENTRADA, "r", encoding="utf-8") as f:
        data = json.load(f)

    novos_dados = []
    sucesso = 0
    total = len(data)

    print(f"Processando {total} documentos...")

    for idx, item in enumerate(data, 1):
        nome_arquivo = item.get("source_filename")
        
        # Faz o Upload e pega a URL
        url = upload_arquivo(client, nome_arquivo)
        
        if url:
            sucesso += 1
            print(f" [{idx}/{total}] Upload OK: {nome_arquivo}")
        else:
            print(f" [{idx}/{total}] Falha/Ignorado: {nome_arquivo}")

        # Monta o novo item apenas com os campos permitidos
        novo_item = {}
        for campo in CAMPOS_FINAL_JSON:
            # Lógica para preencher os campos especiais
            if campo == "url_pdf":
                novo_item[campo] = url
            elif campo == "caminho_arquivo":
                novo_item[campo] = url  # Repete a URL aqui como pedido
            elif campo == "is_legado":
                novo_item[campo] = True
            elif campo == "inteiro_teor":
                # Garante que não seja None
                novo_item[campo] = item.get(campo) or ""
            else:
                # Copia do original
                novo_item[campo] = item.get(campo)

        novos_dados.append(novo_item)

    # Salva o resultado no novo arquivo
    with open(FILE_JSON_SAIDA, "w", encoding="utf-8") as f:
        json.dump(novos_dados, f, ensure_ascii=False, indent=4)
    
    print(f"\n--- Processo Concluído ---")
    print(f"Arquivos processados: {total}")
    print(f"Uploads com sucesso: {sucesso}")
    print(f"JSON gerado em: {FILE_JSON_SAIDA}")

if __name__ == "__main__":
    processar_arquivos()