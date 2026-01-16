import os
import sys
import json
import logging

# Add src to python path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from extract_metadata import extract_metadata, RAW_DATA_DIR

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_fix")

def test_specific_files():
    files_to_test = [
        "14479 5547779-12.2021.8.09.0011 Maria Santíssima da Silva - Oncotype (Ipasgo).pdf",
        "22458 Parecer 5346546-89.2024.8.09.0000 I.F.de A. P. - Cir. de Cabeça e Pescoço.pdf",
        "CONSULTA INTEGRAL - 1030035-57.2025.4.01.3500_compressed.pdf"
    ]

    results = []
    print(f"Testing extraction on {len(files_to_test)} files...")

    for filename in files_to_test:
        pdf_path = os.path.join(RAW_DATA_DIR, filename)
        if not os.path.exists(pdf_path):
            print(f"File not found: {pdf_path}")
            continue
        
        print(f"\nProcessing: {filename}")
        try:
            data = extract_metadata(pdf_path)
            
            # Print key fields
            print(f"  Processo: {data.get('processo')}")
            print(f"  Nota Técnica: {data.get('n_nota_tecnica')}")
            print(f"  Data Envio: {data.get('data_do_envio')}")
            
            results.append(data)
        except Exception as e:
            print(f"  Error: {e}")

    # Save detailed results to examine
    with open("test_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    print("\nFull results saved to test_results.json")

if __name__ == "__main__":
    test_specific_files()
