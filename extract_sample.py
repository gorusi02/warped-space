
import json
import os

def extract_sample(filepath, target_type):
    print(f"Searching for {target_type} in {filepath}...")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    rtype = record.get("record_type")
                    if not rtype:
                        raw = record.get("raw_data", "")
                        if raw: rtype = raw[:2]
                    
                    if rtype == target_type:
                        print(f"\n--- Found {target_type} ---")
                        print(f"Raw Length: {len(record.get('raw_data', '').encode('utf-8'))} bytes (approx)") # Output is unicode so length is char count
                        print(json.dumps(record, ensure_ascii=False))
                        return
                except:
                    continue
    except Exception as e:
        print(e)

if __name__ == "__main__":
    target_file = os.path.join("jra_van_loader", "output_test", "RACE_20240101000000.jsonl")
    extract_sample(target_file, "RA")
    extract_sample(target_file, "SE")
