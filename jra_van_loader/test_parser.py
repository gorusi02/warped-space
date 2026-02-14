import sys
import os
import json

# Add parent dir to path before imports when run as a script.
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from jra_van_loader.parsing import JvParser

def main():
    parser = JvParser()
    
    # Test data from sample
    # JG12025020720250208050103002022105307ナックシュバリエ...
    # Note: "ナックシュバリエ" is 18 bytes (9 chars)
    # Total length: JG(2)+1+8+8+2+2+2+2+10+36+5... = 78 bytes header + body
    
    # Python string len for "ナックシュバリエ" is 8 (Wait, ナックシュバリエ is 8 chars? No, 8 chars)
    # "ナックシュバリエ"
    # Na-kku-shu-ba-ri-e = 8 chars?
    # ナ(1)ッ(1)ク(1)シ(1)ュ(1)バ(1)リ(1)エ(1) = 8 chars.
    # In Shift_JIS, 8 * 2 = 16 bytes. 
    # Wait, spec says 36 bytes for horse name. So padded with spaces.
    
    sample_jsonl_path = os.path.join(os.path.dirname(__file__), "output_test", "RACE_20240101000000.jsonl")
    
    if not os.path.exists(sample_jsonl_path):
        print(f"Sample file not found: {sample_jsonl_path}")
        return

    print(f"Reading sample from {sample_jsonl_path}...")
    
    with open(sample_jsonl_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 5: break
            
            record = json.loads(line)
            raw = record.get("raw_data")
            if not raw: continue
            
            # Remove \r\n
            raw = raw.strip()
            
            print(f"--- Record {i+1} ---")
            parsed = parser.parse(raw)
            print(json.dumps(parsed, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
