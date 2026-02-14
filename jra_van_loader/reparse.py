"""
既存のJSONLデータを新しいスキーマで再パースするスクリプト。
raw_data から拡張フィールドを抽出して新しいJSONLを生成する。
"""
import json
import os
import sys
import logging
from glob import glob

sys.path.insert(0, os.path.dirname(__file__))
from parsing import JvParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def reparse_jsonl(input_dir: str, output_dir: str):
    parser = JvParser()
    os.makedirs(output_dir, exist_ok=True)

    files = glob(os.path.join(input_dir, "*.jsonl"))
    logger.info(f"Found {len(files)} JSONL files in {input_dir}")

    for filepath in files:
        filename = os.path.basename(filepath)
        output_path = os.path.join(output_dir, filename)
        count = 0
        
        with open(filepath, 'r', encoding='utf-8') as fin, \
             open(output_path, 'w', encoding='utf-8') as fout:
            
            for line in fin:
                try:
                    old_rec = json.loads(line)
                    raw_data = old_rec.get('raw_data', '')
                    
                    if not raw_data:
                        fout.write(line)
                        continue
                    
                    # 新しいスキーマで再パース
                    new_rec = parser.parse(raw_data)
                    
                    # 元のメタデータを保持
                    new_rec['fetched_at'] = old_rec.get('fetched_at', '')
                    new_rec['raw_data'] = raw_data
                    
                    fout.write(json.dumps(new_rec, ensure_ascii=False) + '\n')
                    count += 1
                except Exception as e:
                    logger.warning(f"Error parsing line in {filename}: {e}")
                    fout.write(line)
        
        logger.info(f"Re-parsed {filename}: {count} records -> {output_path}")

if __name__ == "__main__":
    input_dir = os.path.join(os.path.dirname(__file__), "output_v2")
    output_dir = os.path.join(os.path.dirname(__file__), "output_v3")
    reparse_jsonl(input_dir, output_dir)
