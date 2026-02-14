import os
import sys
import logging
import win32com.client
import pythoncom

# ロガー設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JVLinkClient:
    """
    JRA-VAN JV-Link クライアント (win32com版)
    DllSurrogateを利用して64bit Pythonから32bit JV-Link COMを操作する。
    """
    def __init__(self, sid: str = "AntigravityPy"):
        self.sid = sid
        self.jv = None
        self.is_open = False
        
        try:
            # COMオブジェクトの生成
            # レジストリ設定(DllSurrogate)が正しければ、ここでdllhost.exe経由でJV-Linkが起動する
            # EnsureDispatchを使うと、タイプライブラリからPythonコードを生成し、厳密な型チェックと引数処理が行われる
            # JVReadなどのByRef引数を正しく扱うために重要
            self.jv = win32com.client.gencache.EnsureDispatch("JVDTLab.JVLink")
            logger.info("JV-Link COM Object created (EnsureDispatch).")
            
            # 初期化
            res = self.jv.JVInit(self.sid)
            if res != 0:
                raise RuntimeError(f"JVInit failed with code: {res}")
            logger.info("JVInit success.")
            
        except Exception as e:
            logger.error(f"Failed to initialize JV-Link: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self, dataspec: str, fromtime: str, option: int = 1):
        """
        データ取得プロセスを開始する
        """
        logger.info(f"Opening JV-Link stream: spec={dataspec}, from={fromtime}")
        
        try:
            # ユーザー提供情報に基づき、6引数で呼び出す
            # JVOpen(dataspec, fromtime, option, readCount, downloadCount, lastTimestamp)
            logger.info("Calling JVOpen with 6 arguments...")
            res = self.jv.JVOpen(dataspec, fromtime, option, 0, 0, "")
            
            ret_code = res
            download_count = 0
            if isinstance(res, tuple):
                ret_code = res[0]
                if len(res) > 2:
                    # ユーザー情報: returnval[2] が downloadcount
                    download_count = res[2]
            
            if ret_code != 0:
                raise RuntimeError(f"JVOpen failed with code: {ret_code}")
                
            self.is_open = True
            logger.info(f"JVOpen success. Download count: {download_count}, Return Code: {ret_code}")
            
            # ダウンロード待ち (JVStatus)
            # JVOpen直後にダウンロードが始まるため、完了するまで待つ必要があるかもしれない
            import time
            while True:
                status = self.jv.JVStatus()
                # JVStatus戻り値:
                # 正の値: ダウンロード済みファイル数
                # 負の値: エラーコード？ いや、仕様では「残りファイル数」の可能性もあるが、通常は進捗
                # ユーザー情報では: downloadcount == status で完了
                
                if status < 0:
                     logger.error(f"JVStatus returned error: {status}")
                     break
                     
                if status == download_count:
                     logger.info(f"Download completed: {status}/{download_count}")
                     break
                else:
                     logger.info(f"Downloading... {status}/{download_count}")
                     time.sleep(1)
            
        except Exception as e:
            logger.error(f"JVOpen/Status Error: {e}")
            self.close()
            raise e

    def read(self):
        """
        データを1行ずつ読み込むジェネレータ
        """
        if not self.is_open:
            return

        # バッファサイズ: JRA-VANの最大レコード長に合わせる (40KB)
        buff_size = 40960
        
        while True:
            try:
                # JVRead("", size, "")
                # win32com + EnsureDispatch では、[in, out] 引数はタプルとして返ってくる
                # 戻り値構造: (RetCode, DataString, BufferSize, Filename)
                result = self.jv.JVRead("", int(buff_size), "")
                
                ret_code = 0
                raw_data = ""
                filename = ""
                
                if isinstance(result, tuple):
                    ret_code = result[0]
                    # Index 1: Data
                    if len(result) > 1:
                         raw_data = result[1]
                    # Index 3: Filename (Index 2 is likely the input buffer size echoed back)
                    if len(result) > 3:
                         filename = result[3]
                else:
                    ret_code = result
                    logger.warning(f"JVRead returned non-tuple: {result}")

                if ret_code == 0: # 完了
                     logger.info("JVRead completed (Code 0).")
                     break
                elif ret_code == -1: # ファイル切り替わり
                     # 新しいファイルへ移動
                     if filename:
                         logger.info(f"File switched to: {filename}")
                     
                     # 切り替わりタイミングでもデータが含まれる場合があるためyield
                     if raw_data: 
                         yield raw_data
                     continue
                elif ret_code > 0: # 正常読み込み
                     # データがあればyield
                     if raw_data:
                         yield raw_data
                else:
                     logger.error(f"JVRead error code: {ret_code}")
                     break
                     
            except Exception as e:
                logger.error(f"JVRead Exception: {e}")
                break

    def close(self):
        """
        接続を閉じる
        """
        if self.jv:
            try:
                self.jv.JVClose()
            except:
                pass
            self.jv = None
        self.is_open = False
