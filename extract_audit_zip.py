
import zipfile
import os
import sys
import re
from pathlib import Path

def sanitize_component(name):
    """
    Sanitiza um componente do caminho (nome de pasta ou arquivo).
    """
    # Remove caracteres ilegais
    name = re.sub(r'[<>:"|?*]', '_', name)
    name = re.sub(r'[\r\n\t]', '', name) # Remove quebras de linha/tab que podem estar no nome
    # Remove pontos e espaços finais
    name = name.strip()
    name = name.rstrip('. ')
    
    # Truncate filename component to 150 chars to be safe (max is usually 255)
    # We keep the extension if possible
    if len(name) > 150:
        base, ext = os.path.splitext(name)
        if len(ext) > 10: # Se extensão for muito longa, provavelmente faz parte do nome
            name = name[:150]
        else:
            name = base[:150-len(ext)] + ext
            
    # Evita nomes reservados
    reserved = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4', 
                'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 
                'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'}
    if name.upper() in reserved:
        name = f"_{name}_"
    return name

def sanitize_path(path_str):
    """
    Reconstroi o caminho sanitizando cada componente e truncando.
    """
    # Normaliza separadores
    path_str = path_str.replace('\\', '/')
    parts = path_str.split('/')
    
    clean_parts = []
    for p in parts:
        if not p or p == '.': 
            continue
        clean_parts.append(sanitize_component(p))
        
    return '/'.join(clean_parts)

def extract_zip_robust(zip_path, extract_to=None):
    zip_path = Path(zip_path).resolve()
    
    if not zip_path.exists():
        print(f"Erro: O arquivo '{zip_path}' não foi encontrado.")
        return False

    if extract_to is None:
        extract_to = zip_path.parent / zip_path.stem
    else:
        extract_to = Path(extract_to).resolve()

    # Prepara diretório base com suporte a caminhos longos
    base_dir_str = str(extract_to)
    if os.name == 'nt' and not base_dir_str.startswith('\\\\?\\'):
        base_dir_str = '\\\\?\\' + base_dir_str
    
    base_dir_path = Path(base_dir_str)

    print(f"Iniciando extração de: {zip_path}")
    print(f"Destino: {extract_to}")

    try:
        base_dir_path.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            count = 0
            errors = 0
            skipped = 0
            
            total_files = len(zip_ref.infolist())
            print(f"Total de arquivos no ZIP: {total_files}")

            for member in zip_ref.infolist():
                try:
                    # Decoding do filename
                    raw_filename = member.filename
                    try:
                        # Tenta decoding comum
                        filename = raw_filename.encode('cp437').decode('utf-8')
                    except:
                        # Fallback
                        filename = raw_filename

                    # Sanitiza o caminho inteiro
                    clean_rel_path = sanitize_path(filename)
                    if not clean_rel_path:
                        skipped += 1
                        continue 

                    target_path = base_dir_path / clean_rel_path
                    
                    if member.is_dir():
                        target_path.mkdir(parents=True, exist_ok=True)
                        continue
                        
                    # Verifica se o caminho final é excessivamente longo mesmo com \\?\ (> 32k)
                    # É raro, mas...
                    
                    target_path.parent.mkdir(parents=True, exist_ok=True)

                    with zip_ref.open(member) as source, open(target_path, "wb") as target:
                        target.write(source.read())
                    
                    count += 1
                    if count % 100 == 0:
                        print(f"Progresso: {count}/{total_files}...", end='\r')

                except Exception as e:
                    errors += 1
                    # Escreve erro em log para não poluir stdout
                    with open("extraction_errors.log", "a", encoding="utf-8") as log:
                        log.write(f"Falha em '{member.filename}': {e}\n")
                    
        print(f"\nExtração concluída!")
        print(f"Sucesso: {count}")
        print(f"Erros: {errors}")
        print(f"Ignorados: {skipped}")
        
        if errors > 0:
            print("Verifique 'extraction_errors.log' para detalhes dos erros.")
            
        return True

    except Exception as e:
        print(f"Erro fatal durante extração: {e}")
        return False

if __name__ == "__main__":
    TARGET_ZIP = r"C:\Users\mlzengo\Documents\TJGO\II SEMESTRE\natjus_extract\resultado_auditoria_docker.zip"
    # Remove log anterior
    if os.path.exists("extraction_errors.log"):
        os.remove("extraction_errors.log")
        
    success = extract_zip_robust(TARGET_ZIP)
    sys.exit(0 if success else 1)
