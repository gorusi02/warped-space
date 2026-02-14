import json
import os
from datetime import datetime
try:
    from .parsing import JvParser
except ImportError:
    from parsing import JvParser

class DataSaver:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.files = {}
        self.parser = JvParser()

    def save(self, raw_data: str):
        # 取得時刻
        fetched_at = datetime.now().isoformat()
        
        # パース実行
        parsed_record = self.parser.parse(raw_data)
        record_type = parsed_record.get("record_type", "UNKNOWN")
        
        # タイムスタンプ付与
        parsed_record["fetched_at"] = fetched_at
        
        # 生データの保持 (ELTのため必須)
        # parsing.py で raw_data を含めていない場合に追加
        if "raw_data" not in parsed_record:
            parsed_record["raw_data"] = raw_data

        # ファイル名決定
        # レコード種別ごとに日次ファイルを作成する
        # 例: RA_20240101.jsonl
        # これにより、BigQueryロード時にテーブル分割やパーティション分割が容易になる
        
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"{record_type}_{date_str}.jsonl"
        filepath = os.path.join(self.output_dir, filename)
        
        if filepath not in self.files:
            # Shift_JISではなくUTF-8で保存 (BigQuery等はUTF-8推奨)
            self.files[filepath] = open(filepath, 'a', encoding='utf-8')
        
        json.dump(parsed_record, self.files[filepath], ensure_ascii=False)
        self.files[filepath].write('\n')

    def close(self):
        for f in self.files.values():
            f.close()
        self.files = {}
