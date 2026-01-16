import json
import sys

# Set output encoding to utf-8
sys.stdout.reconfigure(encoding='utf-8')

filepath = r"C:\Users\mlzengo\Documents\TJGO\II SEMESTRE\natjus_extract\data\processed_data\metadados_extraidos.json"

try:
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
        if isinstance(data, list) and len(data) > 0:
            for key in data[0].keys():
                print(key)
        else:
            print("Data is not a list or is empty.")
except Exception as e:
    print(f"Error: {e}")
