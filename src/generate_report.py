import json
import os
import ijson
from collections import defaultdict

# Configuration
INPUT_FILE = r"data/processed_data/metadados_extraidos.json"
OUTPUT_FILE = r"data/processed_data/relatorio_extracao.md"

# Fields to include in the analysis (excluding 'source_filename' and 'inteiro_teor' as they are base data)
FIELDS_TO_ANALYZE = [
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
]

def is_valid(value):
    """Check if the value counts as a successful extraction."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, (int, float)):
        return True
    return False

def generate_report_streaming(filepath):
    print(f"Reading data from {filepath} (streaming mode)...")
    
    total_pdfs = 0
    num_fields = len(FIELDS_TO_ANALYZE)
    
    # Use simple counters
    extracted_counts = defaultdict(int)
    
    try:
        with open(filepath, 'rb') as f:
            # Iterate over items in the main array
            for item in ijson.items(f, 'item'):
                total_pdfs += 1
                for field in FIELDS_TO_ANALYZE:
                    val = item.get(field)
                    if is_valid(val):
                        extracted_counts[field] += 1
                        
                if total_pdfs % 100 == 0:
                    print(f"Processed stats for {total_pdfs} items...", end='\r')

    except Exception as e:
        print(f"\nError reading JSON stream: {e}")
        return None

    print(f"\nFinished processing {total_pdfs} items.")

    field_stats = {}
    for field in FIELDS_TO_ANALYZE:
        count = extracted_counts[field]
        field_stats[field] = {
            "extracted": count,
            "missing": total_pdfs - count,
            "rate": (count / total_pdfs * 100) if total_pdfs > 0 else 0
        }

    # Aggregate stats
    total_possible = total_pdfs * num_fields
    total_successful = sum(stat["extracted"] for stat in field_stats.values())
    general_success_rate = (total_successful / total_possible * 100) if total_possible > 0 else 0

    fields_100 = [f for f, s in field_stats.items() if s["rate"] == 100]
    fields_partial = [f for f, s in field_stats.items() if 0 < s["rate"] < 100]
    fields_0 = [f for f, s in field_stats.items() if s["rate"] == 0]

    # Generate Markdown Content
    lines = []
    lines.append("# Relatório de Extração de Metadados")
    lines.append("")
    lines.append("## RESUMO GERAL")
    lines.append("")
    lines.append(f"Total de PDFs processados:        {total_pdfs}")
    lines.append(f"Total de campos de metadados:     {num_fields}")
    lines.append(f"Total de extrações possíveis:     {total_possible}")
    lines.append(f"Total de extrações bem-sucedidas: {total_successful}")
    lines.append(f"Taxa de sucesso geral:            {general_success_rate:.2f}%")
    lines.append("")
    
    lines.append(f"Campos com 100% de extração:   {', '.join(fields_100) if fields_100 else 'Nenhum'}")
    lines.append(f"Campos com extração parcial:   {', '.join(fields_partial) if fields_partial else 'Nenhum'}")
    lines.append(f"Campos com 0% de extração:     {', '.join(fields_0) if fields_0 else 'Nenhum'}")
    lines.append("")
    
    lines.append("## DETALHAMENTO POR CAMPO")
    lines.append("")
    lines.append("| Campo | Extraídos | Faltantes | Taxa (%) |")
    lines.append("| :--- | :---: | :---: | :---: |")
    
    for field in FIELDS_TO_ANALYZE:
        s = field_stats[field]
        lines.append(f"| {field} | {s['extracted']} | {s['missing']} | {s['rate']:.2f}% |")

    return "\n".join(lines)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found: {INPUT_FILE}")
        return

    # Use streaming function
    report = generate_report_streaming(INPUT_FILE)
    
    if report:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"Report successfully generated at: {OUTPUT_FILE}")
        print("-" * 30)
        print(report)

if __name__ == "__main__":
    main()
