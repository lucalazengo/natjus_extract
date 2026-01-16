import os
import glob
import json
import re
import csv
import pdfplumber
import logging
from datetime import datetime
import argparse
import traceback

# =========================
# CONFIGURAÇÕES
# =========================

RAW_DATA_DIR = r"C:\Users\mlzengo\Documents\TJGO\II SEMESTRE\natjus_extract\data\raw_data\NT e PARECERES"
PROCESSED_DATA_DIR = r"C:\Users\mlzengo\Documents\TJGO\II SEMESTRE\natjus_extract\data\processed_data"

OUTPUT_JSON = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.json")
OUTPUT_CSV = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.csv")
REPORT_FILE = os.path.join(PROCESSED_DATA_DIR, "relatorio_extracao.md")
CHECKPOINT_FILE = os.path.join(PROCESSED_DATA_DIR, "checkpoint.json")
LOG_FILE = os.path.join(PROCESSED_DATA_DIR, "processamento.log")

os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# =========================
# CHECKPOINT
# =========================

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed": [], "failed": []}


def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=4)


# =========================
# EXTRAÇÃO
# =========================

def extract_metadata(pdf_path):
    filename = os.path.basename(pdf_path)

    metadata = {
        "source_filename": filename,
        "tipo_arquivo": "Nota Técnica" if "N.T" in filename or "NOTA" in filename.upper() else "Parecer",
        "processo": None,
        "Classificação": "Não identificado",
        "Assunto": None,
        "cid": None,
        "n_nota_tecnica": None,
        "desfecho": "Não identificado",
        "inteiro_teor": "",
        "objeto": None,
        "classificador_do_objeto": None,
        "informacao_complementar": None,
        "data_do_envio": None,
        "medicamento_e_insumo": None
    }

    try:
        full_text = ""

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

        metadata["inteiro_teor"] = full_text

        # Processo - Regex expandido e fallback para nome do arquivo
        # Padrão CNJ: NNNNNNN-DD.AAAA.J.TR.OORR
        proc_regex = r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}"
        proc = re.search(proc_regex, full_text)
        
        if proc:
            metadata["processo"] = proc.group(0)
        else:
            # Tenta encontrar no nome do arquivo
            proc_filename = re.search(proc_regex, filename)
            if proc_filename:
                metadata["processo"] = proc_filename.group(0)

        # Nota Técnica / Parecer
        # Aceita "Parecer Técnico N.", "Nota Técnica nº", etc.
        # Permite barra no número (ex: 1234/2023)
        nt_regex = r"(?:Nota\s+T[ée]cnica|Parecer)(?:\s+T[ée]cnico)?\s*(?:n[º°\.]?|número)?\s*(\d+(?:[./-]\d{4})?)"
        nt = re.search(nt_regex, full_text[:2000], re.IGNORECASE)
        
        if nt:
            metadata["n_nota_tecnica"] = nt.group(1)
        else:
            # Tenta extrair do início do nome do arquivo se parecer ser um ID (ex: "22458 Parecer...")
            # Pega números no início do arquivo seguidos de espaço ou delimitador
            nt_filename = re.match(r"^(\d+)\s", filename)
            if nt_filename:
                metadata["n_nota_tecnica"] = nt_filename.group(1)

        # CID
        cid = re.search(r"CID\s*[:\-]?\s*([A-Z]\d{2}(?:\.\d)?)", full_text)
        if cid:
            metadata["cid"] = cid.group(1)

        # Assunto
        assunto = re.search(r"Assunto\s*[:\-]\s*(.*)", full_text)
        if assunto:
            metadata["Assunto"] = assunto.group(1).strip()

        # Data do Envio (geralmente ao final)
        # Procura por "Goiânia, DD de Mes de AAAA"
        # \s* permite casos como "27demaio" (sem espaço) se necessário, ou formatação irregular
        data_regex = r"Goiânia(?:-GO)?\s*,?\s*(\d{1,2})\s*de\s*([A-Za-zç]+)\s*de\s*(\d{4})"
        # Busca nas últimas 1000 caracteres para otimizar e focar no rodapé
        data_envio = re.search(data_regex, full_text[-1000:], re.IGNORECASE)
        
        if data_envio:
            dia, mes, ano = data_envio.groups()
            metadata["data_do_envio"] = f"{dia} de {mes} de {ano}"

        return metadata

    except Exception as e:
        logger.error(f"Erro ao processar {filename}: {e}")
        logger.debug(traceback.format_exc())
        raise


# =========================
# SALVAMENTO
# =========================

def save_json(data):
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def save_csv(data):
    if not data:
        return

    # Remove inteiro_teor para CSV
    csv_data = []
    for row in data:
        r = row.copy()
        r.pop("inteiro_teor", None)
        csv_data.append(r)

    fields = csv_data[0].keys()

    with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(csv_data)


# =========================
# MAIN
# =========================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    checkpoint = load_checkpoint()

    pdf_files = sorted(glob.glob(os.path.join(RAW_DATA_DIR, "*.pdf")))
    pdf_files = [p for p in pdf_files if os.path.basename(p) not in checkpoint["processed"]]

    if args.limit:
        pdf_files = pdf_files[:args.limit]

    logger.info(f"Arquivos pendentes para processamento: {len(pdf_files)}")

    extracted_data = []

    for i, pdf in enumerate(pdf_files, 1):
        name = os.path.basename(pdf)
        logger.info(f"[{i}/{len(pdf_files)}] Processando {name}")

        try:
            data = extract_metadata(pdf)
            extracted_data.append(data)
            checkpoint["processed"].append(name)

        except Exception:
            checkpoint["failed"].append(name)

        finally:
            save_checkpoint(checkpoint)
            save_json(extracted_data)
            save_csv(extracted_data)

    logger.info("Processamento finalizado com sucesso.")
    logger.info(f"Total processados: {len(checkpoint['processed'])}")
    logger.info(f"Total falhas: {len(checkpoint['failed'])}")


if __name__ == "__main__":
    main()
