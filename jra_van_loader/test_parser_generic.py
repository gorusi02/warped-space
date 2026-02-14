
import json
import os
import sys

# Add parent dir to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from jra_van_loader.parsing import JvParser

def main():
    parser = JvParser()
    target_file = os.path.join(os.path.dirname(__file__), "output_test", "RACE_20240101000000.jsonl")
    
    print(f"Testing parser on {target_file}...")
    
    # Check RA and SE
    target_types = ["RA", "SE", "JG"]
    found_types = set()
    
    with open(target_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                raw = record.get("raw_data", "").strip()
                if not raw: continue
                
                parsed = parser.parse(raw)
                rtype = parsed.get("record_type")
                
                if rtype in target_types and rtype not in found_types:
                    print(f"\n--- Parsed {rtype} ---")
                    print(json.dumps(parsed, indent=2, ensure_ascii=False))
                    found_types.add(rtype)
                    
                if len(found_types) >= len(target_types):
                    break
            except Exception as e:
                print(f"Error: {e}")
                continue

if __name__ == "__main__":
    main()
