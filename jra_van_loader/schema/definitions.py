from typing import List, Tuple, Dict, Any, NamedTuple

class Field(NamedTuple):
    name: str
    start: int
    length: int
    dtype: str = "str" # str, int, date, etc.
    desc: str = ""

# JRA-VAN レコード定義
# (フィールド名, 開始位置(0-indexed, CP932バイト), 長さ(byte), データ型, 説明)
#
# 戦略: ELTアプローチ
# 主要フィールドはパースしてカラム化し、残りは raw_body として保持。
# 詳細なパースが必要な場合はBigQuery側で SUBSTR 等を使う。

# 共通ヘッダ (全レコード共通, 27バイト)
COMMON_HEADER: List[Field] = [
    Field("RecordSpec", 0, 2, "str", "レコード種別"),
    Field("DataKubun", 2, 1, "str", "データ区分"),
    Field("MakeDate", 3, 8, "str", "データ作成日"),
    Field("Year", 11, 4, "str", "開催年"),
    Field("MonthDay", 15, 4, "str", "開催月日"),
    Field("JyoCD", 19, 2, "str", "場コード"),
    Field("Kaiji", 21, 2, "str", "回次"),
    Field("Nichiji", 23, 2, "str", "日次"),
    Field("RaceNum", 25, 2, "str", "レース番号"),
]

# RA: レース詳細 (約1272バイト)
# バイト解析に基づくオフセット:
#   27: YoubiCD(1), 28: TokuNum(4)
#   32-91: Hondai(60), 92-151: Fukudai(60), 152-211: Kakko(60)
#   212-331: HondaiEng(120), 332-451: FukudaiEng(120), 452-571: KakkoEng(120)
#   616: TrackCD(2)  ※芝/ダ・左右を示す (10=芝左, 23=ダ右 等)
#   697: Kyori(4)    ※距離 (1200, 1400, ... 3600)
RA_SCHEMA: List[Field] = COMMON_HEADER + [
    Field("YoubiCD", 27, 1, "str", "曜日コード"),
    Field("TokuNum", 28, 4, "str", "特別競走番号"),
    Field("Hondai", 32, 60, "str", "レース名本題"),
    Field("Fukudai", 92, 60, "str", "副題"),
    Field("Kakko", 152, 60, "str", "括弧付き名称"),
    Field("TrackCD", 616, 2, "str", "トラックコード"),
    Field("Kyori", 697, 4, "str", "距離"),
    # 以降は raw_body として保持
]

# SE: 馬毎レース情報 (約555バイト)
SE_SCHEMA: List[Field] = COMMON_HEADER + [
    Field("Wakuban", 27, 1, "str", "枠番"),
    Field("Umaban", 28, 2, "str", "馬番"),
    Field("KettoNum", 30, 10, "str", "血統登録番号"),
    Field("Bamei", 40, 36, "str", "馬名"),
    # 以降は raw_body として保持
]

# JG: 競走馬除外情報
JG_SCHEMA: List[Field] = COMMON_HEADER + [
    Field("HorseID", 27, 10, "str", "血統登録番号"),
    Field("HorseName", 37, 36, "str", "馬名"),
]

RECORD_SPECS: Dict[str, List[Field]] = {
    "JG": JG_SCHEMA,
    "RA": RA_SCHEMA,
    "SE": SE_SCHEMA,
    "HR": COMMON_HEADER,
    "H1": COMMON_HEADER,
    "H6": COMMON_HEADER,
    "WF": COMMON_HEADER,
    "O1": COMMON_HEADER,
    "O2": COMMON_HEADER,
    "O3": COMMON_HEADER,
    "O4": COMMON_HEADER,
    "O5": COMMON_HEADER,
    "O6": COMMON_HEADER,
}
