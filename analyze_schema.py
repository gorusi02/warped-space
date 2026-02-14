import json

def decode(b, start, length):
    chunk = b[start:start+length]
    return chunk.decode('cp932', errors='replace').strip()

def analyze_ra():
    with open('jra_van_loader/output_v2/RA_20260210.jsonl', 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 3:
                break
            rec = json.loads(line)
            raw = rec['raw_data']
            b = raw.encode('cp932', errors='replace')
            print(f"=== RA Record #{i+1} (total {len(b)} bytes) ===")
            print(f"  RecordSpec: {decode(b,0,2)}")
            print(f"  DataKubun:  {decode(b,2,1)}")
            print(f"  MakeDate:   {decode(b,3,8)}")
            print(f"  Year:       {decode(b,11,4)}")
            print(f"  MonthDay:   {decode(b,15,4)}")
            print(f"  JyoCD:      {decode(b,19,2)}")
            print(f"  Kaiji:      {decode(b,21,2)}")
            print(f"  Nichiji:    {decode(b,23,2)}")
            print(f"  RaceNum:    {decode(b,25,2)}")
            # RA固有フィールド推定 (JRA-VAN仕様)
            print(f"  Byte27-31 (YoubiCD+TokuNum): {decode(b,27,5)}")
            print(f"  Byte32-91 (Hondai/60):  [{decode(b,32,60)}]")
            print(f"  Byte92-151 (Fukudai/60): [{decode(b,92,60)}]")
            print(f"  Byte152-211 (Kakko/60):  [{decode(b,152,60)}]")
            print(f"  Byte212-331 (HondaiEng/120): [{decode(b,212,120)}]")
            print(f"  Byte332-451 (FukudaiEng/120): [{decode(b,332,120)}]")
            print(f"  Byte452-571 (KakkoEng/120): [{decode(b,452,120)}]")
            # 距離、トラックコードなど
            print(f"  Byte572-575 (Kyori/4): [{decode(b,572,4)}]")
            print(f"  Byte576-577 (TrackCD/2): [{decode(b,576,2)}]")
            print()

def analyze_se():
    with open('jra_van_loader/output_v2/SE_20260210.jsonl', 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 3:
                break
            rec = json.loads(line)
            raw = rec['raw_data']
            b = raw.encode('cp932', errors='replace')
            print(f"=== SE Record #{i+1} (total {len(b)} bytes) ===")
            print(f"  RecordSpec: {decode(b,0,2)}")
            print(f"  Year:       {decode(b,11,4)}")
            print(f"  RaceNum:    {decode(b,25,2)}")
            # SE固有
            print(f"  Byte27-29 (Umaban/3):   [{decode(b,27,3)}]")
            print(f"  Byte30-39 (KettoNum/10): [{decode(b,30,10)}]")
            print(f"  Byte40-75 (Bamei/36):   [{decode(b,40,36)}]")
            # 上の位置が違う可能性 -> 複数試す
            print(f"  Byte27-36 (10bytes):    [{decode(b,27,10)}]")
            print(f"  Byte37-72 (Bamei?/36):  [{decode(b,37,36)}]")
            print()

analyze_ra()
analyze_se()
