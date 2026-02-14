import json, sys
sys.stdout.reconfigure(encoding='utf-8')

with open('output_v2/RA_20260210.jsonl', 'r', encoding='utf-8') as f:
    for idx, line in enumerate(f):
        if idx >= 5: break
        rec = json.loads(line)
        b = rec['raw_data'].encode('cp932', errors='replace')
        rn = b[25:27].decode('cp932')
        # offset 690-710 周辺を詳しく見る
        print(f"R{rn}:")
        for off in range(690, 720):
            ch = b[off:off+1].decode('cp932', errors='replace')
            print(f"  [{off}] 0x{b[off]:02x} = '{ch}'")
        kyori = b[697:701].decode('cp932', errors='replace')
        # TrackCDはKyoriの直後か直前にあるはず
        before = b[693:697].decode('cp932', errors='replace')
        after = b[701:705].decode('cp932', errors='replace')
        print(f"  -> Kyori=[{kyori}] before=[{before}] after=[{after}]")
        # offset 616周辺も確認
        seg616 = b[614:625].decode('cp932', errors='replace')
        print(f"  -> offset614-624: [{seg616}]")
        print()
