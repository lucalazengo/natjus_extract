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
            total_pages = len(pdf.pages)
            
            # Se tiver muitas páginas, lê apenas o início e o fim para otimizar
            if total_pages > 20:
                pages_to_read = pdf.pages[:10] + pdf.pages[-10:]
            else:
                pages_to_read = pdf.pages

            for page in pages_to_read:
                try:
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"
                except Exception:
                    continue  # Ignora páginas com erro de leitura

        metadata["inteiro_teor"] = full_text

        # ---------------------------------------------------------
        # ESTRATÉGIA DE EXTRAÇÃO ROBUSTA
        # ---------------------------------------------------------
        
        # 1. Normalização do Texto
        # Remove quebras de linha excessivas e espaços duplos para manter 'frases'
        # Mas mantém uma versão completa para buscas que dependem de layout
        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
        text_normalized = ' '.join(lines)

        # 2. Processo - Regex expandido e fallback para nome do arquivo
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

        # 3. Nota Técnica / Parecer
        nt_regex = r"(?:Nota\s+T[ée]cnica|Parecer|Parecer\s+T[ée]cnico)\s*(?:n[º°\.]?|número)?\s*(\d+(?:[./-]\d{4})?)"
        nt = re.search(nt_regex, full_text[:2000], re.IGNORECASE)
        
        if nt:
            metadata["n_nota_tecnica"] = nt.group(1)
        else:
            nt_filename = re.match(r"^(\d+)\s", filename)
            if nt_filename:
                metadata["n_nota_tecnica"] = nt_filename.group(1)

        # 4. CID (Classificação Internacional de Doenças)
        # Tenta padrões comuns, incluindo espaços (E 04.8) e sem separador CID
        cid_regexes = [
            r"CID(?:-10)?\s*[:\-]?\s*([A-Z]\d{2}(?:\.?\d{1})?)", # Padrão
            r"CID\s*[:\-]?\s*([A-Z]\s?\d{2}(?:\.?\d{1})?)",     # Com espaço extra
            r"Diagnóstico.*([A-Z]\d{2}(?:\.\d{1})?)",            # Contexto Diagnóstico
        ]
        
        for regex in cid_regexes:
            cid_match = re.search(regex, full_text, re.IGNORECASE | re.DOTALL)
            if cid_match:
                # Remove espaços internos possíveis (ex: E 04.8 -> E04.8) para padronizar
                cid_clean = cid_match.group(1).replace(" ", "")
                metadata["cid"] = cid_clean
                break

        # 5. Assunto / Objeto / Medicamento
        # Tenta extrair blocos de texto entre cabeçalhos comuns
        
        # Assunto: Geralmente logo no início, após cabeçalho "Assunto:"
        # Captura até encontrar um padrão de próxima seção (ex: "I -", "1.", "DA CONSULTA")
        assunto_regex = r"Assunto\s*[:\-]\s*(.*?)(?=(?:I\s*[\-\)]|1\.|DA IDENTIFICAÇÃO|DA CONSULTA|DADOS DO PROCESSO|$))"
        assunto_match = re.search(assunto_regex, full_text, re.IGNORECASE | re.DOTALL)
        if assunto_match:
            assunto_limpo = assunto_match.group(1).strip().replace('\n', ' ')
            metadata["Assunto"] = assunto_limpo[:500] if len(assunto_limpo) > 500 else assunto_limpo 

        # Medicamento / Objeto
        # Busca por termos chave que indiquem o que está sendo pedido
        termos_objeto = ["Solicita", "Requer", "Prescrição", "Medicamento", "Fármaco", "Procedimento"]
        for termo in termos_objeto:
            # Busca algo como "Solicita: exame oncotype" ou "Medicamento: xxxx"
            # Pega a linha ou frase inteira
            obj_match = re.search(fr"{termo}(?:ção)?\s*[:\-]?\s*([^.;\n]*?[a-zA-Z]{{3,}}[^.;\n]*)", full_text, re.IGNORECASE)
            if obj_match:
                possivel_objeto = obj_match.group(1).strip()
                # Evita capturar textos genéricos de cabeçalho
                if len(possivel_objeto) > 3 and "..." not in possivel_objeto:
                    if metadata["objeto"] is None:
                        metadata["objeto"] = possivel_objeto
                    elif possivel_objeto not in metadata["objeto"]:
                        metadata["objeto"] += f" | {possivel_objeto}"

        # 6. Desfecho / Conclusão
        # Procura seção de conclusão
        conclusao_regex = r"(?:V\)|IV\)|Conclusão|Considerações Finais)\s*[:\-]?\s*(.*?)(?:Goiânia|Este é o parecer|$)"
        conclusao_match = re.search(conclusao_regex, full_text, re.IGNORECASE | re.DOTALL)
        
        if conclusao_match:
            texto_conclusao = conclusao_match.group(1).lower()
            if "favorável" in texto_conclusao and "desfavorável" not in texto_conclusao:
                metadata["desfecho"] = "Favorável"
            elif "desfavorável" in texto_conclusao:
                metadata["desfecho"] = "Desfavorável"
            elif "parcialmente" in texto_conclusao:
                metadata["desfecho"] = "Parcialmente Favorável"
            else:
                metadata["desfecho"] = "Inconclusivo / Não Identificado Explicitamente"
        else:
            # Tenta encontrar palavras soltas perto do fim
            if "favorável" in full_text[-2000:].lower():
                 metadata["desfecho"] = "Favorável (Inferido)"

        # 7. Data do Envio (geralmente ao final)
        data_regex = r"Goiânia(?:-GO)?\s*,?\s*(\d{1,2})\s*de\s*([A-Za-zç]+)\s*de\s*(\d{4})"
        
        # Encontra todas as correspondências e pega a última
        datas_encontradas = re.findall(data_regex, full_text, re.IGNORECASE)
        
        if datas_encontradas:
            dia, mes, ano = datas_encontradas[-1]
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
