
import json
import os
from collections import defaultdict

def count_record_types(filepath):
    counts = defaultdict(int)
    total_lines = 0
    
    print(f"Counting record types in {filepath}...")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                total_lines += 1
                try:
                    record = json.loads(line)
                    # DataSaverが保存したキーは "record_type" を想定
                    # もしなければ raw_data の先頭2文字
                    rtype = record.get("record_type")
                    if not rtype:
                        raw = record.get("raw_data", "")
                        if raw:
                            rtype = raw[:2]
                        else:
                            rtype = "UNKNOWN"
                    
                    counts[rtype] += 1
                            
                    if i % 100000 == 0 and i > 0:
                        print(f"Processed {i} lines...")
                        
                except json.JSONDecodeError:
                    counts["ERROR"] += 1
                    
    except Exception as e:
        print(f"Error: {e}")

    print(f"\n--- Record Type Counts (Total: {total_lines}) ---")
    for rtype, count in sorted(counts.items()):
        print(f"{rtype}: {count}")

if __name__ == "__main__":
    target_file = os.path.join("jra_van_loader", "output_test", "RACE_20240101000000.jsonl")
    count_record_types(target_file)
