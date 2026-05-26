import asyncio
import websockets
import json
import requests
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

# ==========================================
# 1. 設定エリア（必ず書き換えてください！）
# ==========================================
# AisStreamのAPIキー
AIS_API_KEY = "2ec9dc1fe4dfa685ace12e3e3b23fd2c7582628e"

# スターサーバーのドメイン（例: example.com）
MY_DOMAIN = "fair-winds.official.jp"

# Webhookの合言葉（PHPと同じもの）
WEBHOOK_SECRET = "fair_winds_secret_2026"

# ==========================================
# 自動設定エリア（書き換え不要）
# ==========================================
WEBHOOK_URL = f"https://{MY_DOMAIN}/api/update_ais.php"
GET_MMSI_URL = f"https://{MY_DOMAIN}/api/get_tracking_mmsi.php"

# グローバル変数（全機能で共有するデータ）
current_tracking_mmsis = [] # 今監視しているMMSIリスト
needs_resubscribe = False # 注文をやり直す必要があるかどうかのフラグ

# ==========================================
# 2. Renderスリープ防止用のダミーWebサーバー
# ==========================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"AIS Worker is running!")
    
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
# 3. レンタルサーバーからMMSIリストを取得する関数
# ==========================================
def fetch_mmsi_list():
    global current_tracking_mmsis, needs_resubscribe
    try:
        payload = {"secret": WEBHOOK_SECRET}
        response = requests.post(GET_MMSI_URL, json=payload, timeout=10)
        
        if response.status_code == 200:
            new_list = response.json()
            
            if set(new_list) != set(current_tracking_mmsis):
                # 🛠️ ログ強化：更新時に監視対象のMMSIリストを具体的に出力
                print(f"🔄 監視対象のMMSIリストが更新されました: {len(new_list)}隻 {new_list}")
                current_tracking_mmsis = new_list
                needs_resubscribe = True
            return new_list
        else:
            print(f"⚠️ MMSIリストの取得失敗 (サーバー応答 {response.status_code})")
            return []
            
    except Exception as e:
        print(f"⚠️ MMSIリスト取得時に通信エラー: {e}")
        return []

# ==========================================
# 4. AISデータ受信＆転送メイン処理
# ==========================================
async def listen_ais():
    global needs_resubscribe
    
    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                print("✅ AisStreamに接続しました。")
                
                fetch_mmsi_list()
                
                if not current_tracking_mmsis:
                    print("⚠️ 監視対象のMMSIが登録されていません。東京湾の全部乗せでテストします。")
                    subscription_message = {
                        "APIKey": AIS_API_KEY,
                        "BoundingBoxes": [[[34.8, 139.5], [35.7, 140.2]]]
                    }
                else:
                    # 🛠️ ログ強化：開始時に監視対象のMMSIリストを具体的に出力
                    print(f"📡 【ピンポイント監視】{len(current_tracking_mmsis)}隻の追跡を開始します。対象MMSI: {current_tracking_mmsis}")
                    subscription_message = {
                        "APIKey": AIS_API_KEY,
                        "FiltersShipMMSI": current_tracking_mmsis
                    }
                
                await websocket.send(json.dumps(subscription_message))
                needs_resubscribe = False

                async for message_json in websocket:
                    if needs_resubscribe:
                        # 🛠️ ログ強化：注文出し直し時にも最新リストを出力
                        print(f"📡 MMSIリストが更新されたので、新しい注文を送信します。対象MMSI: {current_tracking_mmsis}")
                        new_subscription_message = {
                            "APIKey": AIS_API_KEY,
                            "FiltersShipMMSI": current_tracking_mmsis
                        }
                        await websocket.send(json.dumps(new_subscription_message))
                        needs_resubscribe = False
                    
                    message = json.loads(message_json)
                    
                    if message["MessageType"] == "PositionReport":
                        meta = message.get("MetaData", {})
                        report = message.get("Message", {}).get("PositionReport", {})
                        
                        mmsi = meta.get("MMSI")
                        ship_name = meta.get("ShipName", "不明").strip()
                        lat = report.get("Latitude")
                        lon = report.get("Longitude")
                        cog = report.get("Cog")
                        sog = report.get("Sog")
                        status = report.get("NavigationalStatus", "不明")

                        if not current_tracking_mmsis or str(mmsi) in current_tracking_mmsis:
                            print(f"🚢 ターゲット受信！ [{mmsi}] {ship_name} (Lat: {lat}, Lon: {lon})")
                            
                            payload = {
                                "secret": WEBHOOK_SECRET,
                                "mmsi": mmsi,
                                "lat": lat,
                                "lon": lon,
                                "cog": cog,
                                "sog": sog,
                                "status": status,
                                "dest": meta.get("Destination", "").strip(),
                                "eta": meta.get("ETA", "").strip()
                            }
                            
                            try:
                                response = requests.post(WEBHOOK_URL, json=payload, timeout=5)
                                if response.status_code == 200:
                                    # 🛠️ ログ強化：データベースへの送信が成功した時に完了ログを出力
                                    print(f" 🚀 データベースへの送信完了: {response.text}")
                                else:
                                    print(f" ❌ 転送拒否: サーバー応答 {response.status_code} ({response.text})")
                            except Exception as e:
                                print(f" ❌ 通信エラー: {e}")

        except Exception as e:
            print(f"⚠️ 接続エラー。3秒後に再接続します... (理由: {e})")
            await asyncio.sleep(3)

# ==========================================
# 5. 【完全自動化】5分おきにMMSIリストを確認する定期タスク
# ==========================================
def run_mmsi_updater():
    print("⏰ MMSIリストの自動更新タスク（5分おき）を開始します。")
    time.sleep(30)
    while True:
        fetch_mmsi_list()
        time.sleep(300)

# ==========================================
# 6. 実行開始
# ==========================================
if __name__ == "__main__":
    threading.Thread(target=run_dummy_server, daemon=True).start()
    threading.Thread(target=run_mmsi_updater, daemon=True).start()
    asyncio.run(listen_ais())

