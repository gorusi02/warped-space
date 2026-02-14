import logging
from typing import Dict, Any, List
# 相対インポートではなく絶対インポートにする（スクリプト実行時のトラブル回避）
# ただしパッケージ構造に依存するため、実行環境に合わせて調整が必要だが
# ここでは jra_van_loader パッケージ内であることを前提とする
try:
    from .schema.definitions import RECORD_SPECS, Field
except ImportError:
    # 単体テストなどでパスが通っていない場合
    from schema.definitions import RECORD_SPECS, Field

logger = logging.getLogger(__name__)

class JvParser:
    """
    JV-Linkの固定長データをパースするクラス
    """
    def __init__(self):
        self.specs = RECORD_SPECS

    def parse(self, raw_data: str) -> Dict[str, Any]:
        """
        生データ文字列をパースして辞書を返す
        """
        if not raw_data or len(raw_data) < 2:
            return {"raw_data": raw_data, "error": "Too short"}

        record_spec = raw_data[0:2]
        
        if record_spec not in self.specs:
            # 未定義のレコード種別は生データのまま返す
            return {
                "record_type": record_spec,
                "raw_data": raw_data, 
                "_parsed": False
            }

        schema = self.specs[record_spec]
        parsed_data = {"record_type": record_spec, "_parsed": True} # メタデータ

        # byteエンコーディングしてバイト位置でスライスする必要がある
        # JRA-VANデータはShift_JIS (CP932)
        try:
            raw_bytes = raw_data.encode('cp932')
        except UnicodeEncodeError:
            logger.warning(f"Failed to encode raw data to cp932. Parsing as string (positions may be off).")
            return {"record_type": record_spec, "raw_data": raw_data, "error": "Encoding failed"}

        for field in schema:
            # バイト位置で抽出
            start = field.start
            length = field.length
            
            if start + length > len(raw_bytes):
                # データ長不足
                val_bytes = raw_bytes[start:]
            else:
                val_bytes = raw_bytes[start : start + length]
            
            # デコード
            try:
                val_str = val_bytes.decode('cp932').strip()
            except UnicodeDecodeError:
                val_str = val_bytes.decode('cp932', errors='replace').strip()
            
            # 型変換 (現時点では全て文字列だが、将来的にはint/date対応も可)
            parsed_data[field.name] = val_str

        # 定義されていない残りの部分を raw_body として保持 (ELT用)
        # 最後のフィールドの終了位置・定義の最大終了位置を探す
        max_end = max((f.start + f.length for f in schema), default=0)
        
        if max_end < len(raw_bytes):
            body_bytes = raw_bytes[max_end:]
            try:
                # ボディ部はバイナリデータを含む可能性もあるが、テキストベースならデコード
                # エラー時は replace
                parsed_data["raw_body"] = body_bytes.decode('cp932', errors='replace').strip()
            except:
                parsed_data["raw_body"] = str(body_bytes) # Fallback

        # 生データも含めるか？ -> DataSaver側で制御

        return parsed_data
