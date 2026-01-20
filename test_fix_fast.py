import os
import sys
import json
import logging
import pdfplumber
import re

# Add src to python path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from extract_metadata import extract_metadata, RAW_DATA_DIR

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_fix")

def test_fast():
    # Only small files
    files_to_test = [
        "14479 5547779-12.2021.8.09.0011 Maria Santíssima da Silva - Oncotype (Ipasgo).pdf",
        "22458 Parecer 5346546-89.2024.8.09.0000 I.F.de A. P. - Cir. de Cabeça e Pescoço.pdf"
    ]

    results = []
    print(f"Testing extraction on {len(files_to_test)} small files...")

    for filename in files_to_test:
        pdf_path = os.path.join(RAW_DATA_DIR, filename)
        if not os.path.exists(pdf_path):
            print(f"File not found: {pdf_path}")
            continue
        
        print(f"\nProcessing: {filename}")
        try:
            data = extract_metadata(pdf_path)
            
            print(f"  Processo: {data.get('processo')}")
            print(f"  Nota Técnica: {data.get('n_nota_tecnica')}")
            print(f"  Data Envio: {data.get('data_do_envio')}")
            
            results.append(data)
        except Exception as e:
            print(f"  Error: {e}")

    # Special test for the large file: manually extract first/last pages to test regex logic
    large_file = "CONSULTA INTEGRAL - 1030035-57.2025.4.01.3500_compressed.pdf"
    large_path = os.path.join(RAW_DATA_DIR, large_file)
    if os.path.exists(large_path):
        print(f"\nProcessing large file (partial): {large_file}")
        try:
            full_text = ""
            with pdfplumber.open(large_path) as pdf:
                # Read first 5 and last 5 pages
                pages_to_read = list(range(min(5, len(pdf.pages))))
                if len(pdf.pages) > 10:
                    pages_to_read.extend(range(len(pdf.pages)-5, len(pdf.pages)))
                
                for i in pages_to_read:
                    try:
                        text = pdf.pages[i].extract_text()
                        if text:
                            full_text += text + "\n"
                    except:
                        pass
            
            # Now apply regex logic copied from extract_metadata (or just import if I refactored)
            metadata = {}
            metadata["inteiro_teor"] = full_text # partial

            # Processo
            proc_regex = r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}"
            proc = re.search(proc_regex, full_text)
            if proc:
                metadata["processo"] = proc.group(0)
            else:
                proc_filename = re.search(proc_regex, large_file)
                if proc_filename:
                    metadata["processo"] = proc_filename.group(0)

            # Nota Técnica
            nt_regex = r"(?:Nota\s+T[ée]cnica|Parecer)(?:\s+T[ée]cnico)?\s*(?:n[º°\.]?|número)?\s*(\d+(?:[./-]\d{4})?)"
            nt = re.search(nt_regex, full_text[:2000], re.IGNORECASE)
            if nt:
                metadata["n_nota_tecnica"] = nt.group(1)
            else:
                nt_filename = re.match(r"^(\d+)\s", large_file)
                if nt_filename:
                    metadata["n_nota_tecnica"] = nt_filename.group(1)

            # Data do Envio
            data_regex = r"Goiânia(?:-GO)?\s*,?\s*(\d{1,2})\s*de\s*([A-Za-zç]+)\s*de\s*(\d{4})"
            data_envio = re.search(data_regex, full_text[-1000:], re.IGNORECASE)
            if data_envio:
                dia, mes, ano = data_envio.groups()
                metadata["data_do_envio"] = f"{dia} de {mes} de {ano}"

            print(f"  Processo: {metadata.get('processo')}")
            print(f"  Nota Técnica: {metadata.get('n_nota_tecnica')}")
            print(f"  Data Envio: {metadata.get('data_do_envio')}")
            results.append(metadata)

        except Exception as e:
            print(f"  Error processing large file: {e}")

    with open("test_results_fast.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    print("\nResults saved to test_results_fast.json")

if __name__ == "__main__":
    test_fast()
