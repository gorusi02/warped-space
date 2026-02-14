import json, sys
sys.stdout.reconfigure(encoding='utf-8')

print("=== RA Records ===")
with open('output_v3/RA_20260210.jsonl','r',encoding='utf-8') as f:
    for i,line in enumerate(f):
        if i >= 5: break
        r = json.loads(line)
        hondai = r.get("Hondai","")
        kyori = r.get("Kyori","")
        track = r.get("TrackCD","")
        rn = r.get("RaceNum","")
        print(f"  RA#{i+1}: RaceNum={rn} Hondai=[{hondai}] Kyori=[{kyori}] Track=[{track}]")

print("\n=== SE Records ===")
with open('output_v3/SE_20260210.jsonl','r',encoding='utf-8') as f:
    for i,line in enumerate(f):
        if i >= 5: break
        r = json.loads(line)
        bamei = r.get("Bamei","")
        umaban = r.get("Umaban","")
        wakuban = r.get("Wakuban","")
        ketto = r.get("KettoNum","")
        print(f"  SE#{i+1}: Wakuban={wakuban} Umaban={umaban} Bamei=[{bamei}] KettoNum={ketto}")
