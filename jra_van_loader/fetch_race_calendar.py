
import win32com.client
import sys
import datetime

# コンソール出力の文字コードをUTF-8に設定
sys.stdout.reconfigure(encoding='utf-8')

def fetch_race_calendar():
    print("=== JRA-VAN レース開催情報取得テスト ===")
    try:
        # 1. JV-Linkオブジェクトの生成
        jv = win32com.client.Dispatch("JVDTLab.JVLink")
        
        # 2. JV-Linkの初期化
        res = jv.JVInit("AntigravityCheck") 
        if res != 0:
            print(f"[エラー] 初期化に失敗しました。エラーコード: {res}")
            return

        print("[成功] JV-Linkの初期化に成功しました。")

        # 3. 直近のデータ(RACE詳細)を取得してみる
        # dataspec: "RACE" (レース詳細)
        # fromtime: "20240101000000" (YYYYMMDDHHMMSS形式)
        # option: 1 (通常)
        
        # 2024年1月1日以降のデータ取得を試みる
        target_date = "20240101000000"
        dataspec = "RACE" 
        
        print(f"データ取得要求: {dataspec} 自: {target_date}")

        # JVOpen: データ取得開始
        # win32comの戻り値はタプル (return_code, dataspec, size, latest_timestamp) の形式になる可能性がある
        res = jv.JVOpen(dataspec, target_date, 1)

        ret_code = res
        if isinstance(res, tuple):
            ret_code = res[0]
            print(f"[デバッグ] JVOpen戻り値（タプル）: {res}")
        else:
            print(f"[デバッグ] JVOpen戻り値: {res}")

        if ret_code != 0:
            print(f"[エラー] JVOpenに失敗しました。エラーコード: {ret_code}")
            # エラーコードの簡単な解説
            if ret_code == -1: print("  -> 該当データがありません")
            elif ret_code == -3: print("  -> ダウンロード処理に失敗しました")
            elif ret_code == -201: print("  -> 直前のJVOpenが処理中です")
            jv.JVClose()
            return
            
        print("[成功] データ取得を開始しました。読み込みます...")

        # 4. データの読み出し (JVRead)
        # バッファサイズは大きめにとる
        buff_size = 100000 
        # ファイル名(空でも良いが、参照渡しのために変を用意)
        filename = "" 
        
        count = 0
        max_count = 5 # 最初の5件だけ表示して終了

        while True:
            try:
                # JVRead: 1行ずつデータを読み込む
                # win32comでは戻り値として (return_code, buff, filename) が返ってくることが多い
                # もしくはmakepyの状況によっては (return_code, buff, filename, size?)
                
                # 呼び出し
                res_read = jv.JVRead(buff_size, filename)
                
                read_code = res_read
                data = ""
                fname = ""
                
                if isinstance(res_read, tuple):
                    read_code = res_read[0]
                    # バッファの中身(文字列)があれば取得
                    if len(res_read) > 1:
                        data = res_read[1]
                    if len(res_read) > 2:
                        fname = res_read[2]
                
                if read_code == 0: # データの終わり（正常終了）
                    if len(data) > 0:
                         # 最後のデータがある場合も考慮（通常JVRead終了時は空だが念のため）
                         pass
                    print("データの読み込みが完了しました(終了コード0)。")
                    break
                elif read_code == -1: # ファイルの切り替わりなど（EOF相当だが継続可能）
                     # ファイル切り替わりなどの通知
                     print(f"  -> ファイル等の切り替わり(-1): {fname}")
                     if len(data) > 0:
                        # データが含まれている場合は処理する
                        count += 1
                        record_id = data[:2]
                        print(f"データ{count}: 種別={record_id} 長さ={len(data)}")
                     continue 
                elif read_code < 0:
                    print(f"[エラー] 読み込み中にエラーが発生: {read_code}")
                    break
                
                if read_code > 0:
                    # データ取得成功 (読み込んだバイト数が入ることもあるが、win32comだと単純に正の値=成功扱いかも)
                    # ドキュメント上は正の値=読み込んだサイズ
                    
                    count += 1
                    # JRA-VANのデータは固定長なので、先頭の識別子などを表示してみる
                    # データ種別IDは先頭2文字
                    record_id = data[:2]
                    print(f"データ{count}: 種別={record_id} 長さ={len(data)}")
                    # 最初の100文字だけ表示
                    try:
                        print(f"  内容(抜粋): {data[:100]}...")
                    except:
                        pass
                    
                    if count >= max_count:
                        print(f"{max_count}件取得したのでテストを終了します。")
                        break

            except Exception as e:
                print(f"[例外] JVRead呼び出し時にエラー: {e}")
                break

        # 5. JVClose
        jv.JVClose()
        print("JV-Linkを閉じました。")

    except Exception as e:
        print(f"[致命的エラー] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_race_calendar()
