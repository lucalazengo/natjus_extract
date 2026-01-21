import json
import os

# Configuration
INPUT_FILE = r"/Users/universo369/Documents/UNIVERSO_369/U369_root/RESIDENCIA EM TI - TJGO + TJGO/TJGO/II SEMESTRE/notas_legacy/natjus_extract/data/processed_data/metadados_extraidos.json"
OUTPUT_FILE = r"/Users/universo369/Documents/UNIVERSO_369/U369_root/RESIDENCIA EM TI - TJGO + TJGO/TJGO/II SEMESTRE/notas_legacy/natjus_extract/data/processed_data/relatorio_extracao.md"

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
    "medicamento_e_insumo"
]

def load_data(filepath):
    print(f"Loading data from {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

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

def generate_report(data):
    total_pdfs = len(data)
    num_fields = len(FIELDS_TO_ANALYZE)
    
    field_stats = {}
    
    # Calculate stats per field
    for field in FIELDS_TO_ANALYZE:
        extracted_count = 0
        for entry in data:
            val = entry.get(field)
            if is_valid(val):
                extracted_count += 1
        
        field_stats[field] = {
            "extracted": extracted_count,
            "missing": total_pdfs - extracted_count,
            "rate": (extracted_count / total_pdfs * 100) if total_pdfs > 0 else 0
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
    # Table Header
    # Campo | Extraídos | Faltantes | Taxa (%)
    # Using a markdown table usually, but the user showed a text layout. 
    # I will use a markdown table for clarity as it renders well.
    
    lines.append("| Campo | Extraídos | Faltantes | Taxa (%) |")
    lines.append("| :--- | :---: | :---: | :---: |")
    
    # Sort fields for better readability? Or keep prioritized order? 
    # Alphabetical might be nice, or by rate. Let's keep the defined order.
    for field in FIELDS_TO_ANALYZE:
        s = field_stats[field]
        lines.append(f"| {field} | {s['extracted']} | {s['missing']} | {s['rate']:.2f}% |")

    report_content = "\n".join(lines)
    
    return report_content

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found: {INPUT_FILE}")
        return

    data = load_data(INPUT_FILE)
    if not isinstance(data, list):
        print("Error: JSON data is not a list.")
        return

    report = generate_report(data)
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"Report successfully generated at: {OUTPUT_FILE}")
    print("-" * 30)
    print(report)

if __name__ == "__main__":
    main()
