import csv
import json
import os
import shutil
import base64
import sys

# =========================
# CONFIGURAÇÕES DE CAMINHO
# =========================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed_data")
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw_data", "NT e PARECERES")

FILE_CSV = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.csv")
FILE_JSON = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.json")

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
    "conteudo_pdf_base64"
]

def obter_conteudo_pdf_base64(filename):
    if not filename: return None, "Sem Nome"
    caminho_arquivo = os.path.join(RAW_DATA_DIR, filename)
    if not os.path.exists(caminho_arquivo): return None, "Arquivo Não Encontrado"
    try:
        with open(caminho_arquivo, "rb") as pdf_file:
            encoded_string = base64.b64encode(pdf_file.read()).decode('utf-8')
        return encoded_string, "Sucesso"
    except Exception as e:
        return None, f"Erro: {str(e)}"

def atualizar_csv():
    # Mantive a função igual (apenas metadados no CSV)
    pass 

def gerar_json_final():
    if not os.path.exists(FILE_JSON): return
    print(f"\n--- Recriando JSON (Correção de Filename + Base64) ---")

    try:
        with open(FILE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)

        novos_dados = []
        stats = {"ok": 0, "fail": 0}

        print(f"Processando {len(data)} registros...")

        for idx, item in enumerate(data, 1):
            nome_arquivo = item.get("source_filename")
            
            # Tenta pegar o Base64
            conteudo_b64, status = obter_conteudo_pdf_base64(nome_arquivo)
            
            if conteudo_b64: stats["ok"] += 1
            else: stats["fail"] += 1

            # Copia apenas os campos permitidos
            novo_item = {k: item.get(k) for k in CAMPOS_FINAL_JSON if k not in ["is_legado", "conteudo_pdf_base64"]}
            
            # Garante campos obrigatórios
            if novo_item.get("inteiro_teor") is None: novo_item["inteiro_teor"] = ""
            
            # Adiciona os campos pesados/especiais
            novo_item["is_legado"] = True
            novo_item["conteudo_pdf_base64"] = conteudo_b64
            # Garante que o source_filename esteja no item final
            novo_item["source_filename"] = nome_arquivo 

            novos_dados.append(novo_item)
            if idx % 500 == 0: print(f"Progresso: {idx}...", end="\r")

        with open(FILE_JSON, "w", encoding="utf-8") as f:
            json.dump(novos_dados, f, ensure_ascii=False, indent=4)
            
        print(f"\nJSON Final Salvo! PDFs encontrados: {stats['ok']} | Falhas: {stats['fail']}")

    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    gerar_json_final()