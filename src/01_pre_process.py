import csv
import json
import os
import shutil

# =========================
# CONFIGURAÇÕES
# =========================

PROCESSED_DATA_DIR = r"/Users/universo369/Documents/UNIVERSO_369/U369_root/RESIDENCIA EM TI - TJGO + TJGO/TJGO/II SEMESTRE/notas_legacy/natjus_extract/data/processed_data"

FILE_CSV = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.csv")
FILE_JSON = os.path.join(PROCESSED_DATA_DIR, "metadados_extraidos.json")

NOME_CAMPO = "tipo"
VALOR_PADRAO = "legado"

def ler_csv_robust(filepath):
    """
    Tenta ler com várias codificações e trata linhas 'quebradas' (com mais colunas que o cabeçalho).
    """
    encodings_to_try = ['latin1', 'utf-8-sig', 'utf-16', 'cp1252']
    
    for enc in encodings_to_try:
        try:
            print(f"Tentando ler com codificação: {enc}...")
            with open(filepath, mode='r', encoding=enc) as f:
                # restkey='Extra' captura colunas excedentes para não quebrar a leitura inicial
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
        print(f"CSV não encontrado: {FILE_CSV}")
        return

    temp_csv = FILE_CSV + ".temp"
    print(f"Iniciando correção e atualização do CSV...")

    try:
        # 1. Leitura
        fieldnames, rows, encoding_detectado = ler_csv_robust(FILE_CSV)
        print(f"Sucesso! Arquivo lido como: {encoding_detectado}")

        # 2. Prepara cabeçalho
        if NOME_CAMPO not in fieldnames:
            fieldnames.append(NOME_CAMPO)
        
        # Garante que não temos campos 'None' ou 'Extra' no cabeçalho de escrita
        if 'Extra' in fieldnames:
            fieldnames.remove('Extra')

        # 3. Escrita (Forçando UTF-8-SIG para resolver problemas futuros)
        with open(temp_csv, mode='w', encoding='utf-8-sig', newline='') as f_out:
            # extrasaction='ignore' é o segredo: ele ignora campos que não estão no cabeçalho (a sujeira)
            writer = csv.DictWriter(f_out, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()

            count = 0
            for row in rows:
                # Adiciona o novo campo
                if NOME_CAMPO not in row or not row[NOME_CAMPO]:
                    row[NOME_CAMPO] = VALOR_PADRAO
                
                # Remove a chave 'Extra' se ela foi criada na leitura (limpeza de sujeira)
                if 'Extra' in row:
                    del row['Extra']
                
                # Remove chaves None (erros de parsing do Python)
                if None in row:
                    del row[None]

                writer.writerow(row)
                count += 1
        
        # 4. Substituição
        shutil.move(temp_csv, FILE_CSV)
        print(f"CSV corrigido e salvo em UTF-8! ({count} linhas processadas)")

    except Exception as e:
        print(f"CRÍTICO - Erro ao atualizar CSV: {e}")
        # Importante: apaga o temp se der erro para não deixar lixo
        if os.path.exists(temp_csv):
            os.remove(temp_csv)

def main():
    print("=== Iniciando Correção Final ===")
    atualizar_csv()
    # Não precisamos rodar o JSON de novo se ele já deu sucesso na última vez
    print("=== Processo concluído ===")

if __name__ == "__main__":
    main()