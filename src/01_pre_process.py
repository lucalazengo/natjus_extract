import csv
import json
import os
import shutil

# =========================
# CONFIGURAÇÕES DE CAMINHO
# =========================

# 1. Pega o diretório onde o script está (pasta src)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Define a raiz do projeto (um nível acima de src)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# 3. Caminho para os dados
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed_data")

FILE_CSV = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.csv")
FILE_JSON = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.json")

# Definições do campo e valor
NOME_CAMPO = "tipo"
VALOR_PADRAO = "legado"

def ler_csv_robust(filepath):
    """
    Tenta ler com várias codificações e trata linhas 'quebradas'.
    """
    encodings_to_try = ['latin1', 'utf-8-sig', 'utf-16', 'cp1252']
    
    for enc in encodings_to_try:
        try:
            print(f"Tentando ler CSV com codificação: {enc}...")
            with open(filepath, mode='r', encoding=enc) as f:
                reader = csv.DictReader(f, restkey='Extra')
                fieldnames = reader.fieldnames
                rows = list(reader) 
                return fieldnames, rows, enc
        except UnicodeError:
            continue
        except Exception as e:
            print(f"Erro genérico com {enc}: {e}")
            continue
            
    raise ValueError("Não foi possível identificar a codificação do arquivo CSV.")

def atualizar_csv():
    if not os.path.exists(FILE_CSV):
        print(f"ERRO: CSV não encontrado em: {FILE_CSV}")
        return

    temp_csv = FILE_CSV + ".temp"
    print(f"--- Atualizando CSV ---")

    try:
        fieldnames, rows, encoding_detectado = ler_csv_robust(FILE_CSV)
        print(f"Sucesso! CSV lido como: {encoding_detectado}")

        if NOME_CAMPO not in fieldnames:
            fieldnames.append(NOME_CAMPO)
        
        if 'Extra' in fieldnames:
            fieldnames.remove('Extra')

        with open(temp_csv, mode='w', encoding='utf-8-sig', newline='') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()

            count = 0
            for row in rows:
                if NOME_CAMPO not in row or not row[NOME_CAMPO]:
                    row[NOME_CAMPO] = VALOR_PADRAO
                
                # Limpeza de sujeira
                if 'Extra' in row: del row['Extra']
                if None in row: del row[None]

                writer.writerow(row)
                count += 1
        
        shutil.move(temp_csv, FILE_CSV)
        print(f"CSV atualizado e salvo em UTF-8! ({count} linhas processadas)")

    except Exception as e:
        print(f"CRÍTICO - Erro ao atualizar CSV: {e}")
        if os.path.exists(temp_csv):
            os.remove(temp_csv)

def atualizar_json():
    if not os.path.exists(FILE_JSON):
        print(f"ERRO: JSON não encontrado em: {FILE_JSON}")
        return

    print(f"--- Atualizando JSON ---")

    try:
        with open(FILE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)

        count = 0
        modificado = False
        for item in data:
            if NOME_CAMPO not in item:
                item[NOME_CAMPO] = VALOR_PADRAO
                count += 1
                modificado = True
        
        if modificado:
            with open(FILE_JSON, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"JSON atualizado! ({count} registros modificados)")
        else:
            print("JSON já estava atualizado.")

    except Exception as e:
        print(f"Erro ao atualizar JSON: {e}")

def main():
    print("=== Iniciando Correção Final (CSV + JSON) ===")
    print(f"Raiz do projeto: {PROJECT_ROOT}")
    
    atualizar_csv()
    atualizar_json() # Agora incluído explicitamente
    
    print("=== Processo concluído ===")

if __name__ == "__main__":
    main()