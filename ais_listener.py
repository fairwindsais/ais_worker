import asyncio
import websockets
import json
import requests
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# ==========================================
# 1. 設定エリア（必ず書き換えてください！）
# ==========================================
# AisStreamのAPIキー
AIS_API_KEY = "2ec9dc1fe4dfa685ace12e3e3b23fd2c7582628e"

# スターサーバーのデータ受け取りURL（api/update_ais.phpのURL）
WEBHOOK_URL = "https://fair-winds.official.jp/api/update_ais.php"

# ==========================================
# 2. Renderスリープ防止用のダミーWebサーバー
# ==========================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"AIS Worker is running!")
    
    # Cronからの curl -I (HEADリクエスト) に 200 OK で返すための追加設定
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

def run_dummy_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), DummyHandler)
    print(f"✅ ダミーWebサーバー起動 (Port: {port})")
    server.serve_forever()

# ==========================================
# 3. AISデータ受信＆転送メイン処理
# ==========================================
async def listen_ais():
    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                print("✅ AisStreamに接続しました。東京湾のデータを受信待機中...")
                
                # 絞り込みなし！東京湾のすべての船のデータを要求する
                subscription_message = {
                    "APIKey": AIS_API_KEY,
                    "BoundingBoxes": [[[34.8, 139.5], [35.7, 140.2]]]
                }
                
                await websocket.send(json.dumps(subscription_message))

                async for message_json in websocket:
                    message = json.loads(message_json)
                    
                    if message["MessageType"] == "PositionReport":
                        meta = message.get("MetaData", {})
                        report = message.get("Message", {}).get("PositionReport", {})
                        
                        mmsi = meta.get("MMSI")
                        ship_name = meta.get("ShipName", "不明")
                        lat = report.get("Latitude")
                        lon = report.get("Longitude")
                        status = report.get("NavigationalStatus", "不明")

                        print(f"🚢 [{mmsi}] {ship_name} の位置情報を受信！ => 転送中...")
                        
                        # レンタルサーバーへデータを転送
                        payload = {
                            "mmsi": mmsi,
                            "lat": lat,
                            "lon": lon,
                            "status": status,
                            "dest": "",
                            "eta": ""
                        }
                        
                        try:
                            # タイムアウトを設けてサーバーの負担を減らす
                            response = requests.post(WEBHOOK_URL, data=payload, timeout=5)
                        except Exception as e:
                            print(f" ❌ サーバーへの転送失敗: {e}")

        except Exception as e:
            # 接続が切れた場合、理由を表示して3秒後に自動復活する
            print(f"⚠️ 接続エラー。3秒後に再接続します... (理由: {e})")
            await asyncio.sleep(3)

# ==========================================
# 4. 実行開始
# ==========================================
if __name__ == "__main__":
    # ダミーWebサーバーを別スレッドで起動
    threading.Thread(target=run_dummy_server, daemon=True).start()
    
    # AIS受信の無限ループを開始
    asyncio.run(listen_ais())
