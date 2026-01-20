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
logger = logging.getLogger("test_fix_robust")

def test_robust():
    files_to_test = [
        "14479 5547779-12.2021.8.09.0011 Maria Santíssima da Silva - Oncotype (Ipasgo).pdf",
        "22458 Parecer 5346546-89.2024.8.09.0000 I.F.de A. P. - Cir. de Cabeça e Pescoço.pdf"
    ]

    results = []
    print(f"Testing ROBUST extraction on {len(files_to_test)} small files...")

    for filename in files_to_test:
        pdf_path = os.path.join(RAW_DATA_DIR, filename)
        if not os.path.exists(pdf_path):
            print(f"File not found: {pdf_path}")
            continue
        
        print(f"\nProcessing: {filename}")
        try:
            data = extract_metadata(pdf_path)
            
            print(f"  CID: {data.get('cid')}")
            print(f"  Assunto: {data.get('Assunto')[:100]}...") # Truncate for display
            print(f"  Objeto: {data.get('objeto')}")
            print(f"  Desfecho: {data.get('desfecho')}")
            
            results.append(data)
        except Exception as e:
            print(f"  Error: {e}")

    with open("test_results_robust.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    print("\nResults saved to test_results_robust.json")

if __name__ == "__main__":
    test_robust()
