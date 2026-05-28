import asyncio
import websockets
import json
import requests
import os
import threading
import time
import builtins
from http.server import BaseHTTPRequestHandler, HTTPServer

# ==========================================
# 🌟 Render用：ログを溜め込まず即座に画面に表示する魔法
# ==========================================
def print(*args, **kwargs):
    kwargs["flush"] = True
    builtins.print(*args, **kwargs)

# ==========================================
# 1. 設定エリア（必ず書き換えてください！）
# ==========================================
AIS_API_KEY = "2ec9dc1fe4dfa685ace12e3e3b23fd2c7582628e"
MY_DOMAIN = 'fair-winds.official.jp'
WEBHOOK_SECRET = "fair_winds_secret_2026"

# ==========================================
# 自動設定エリア（書き換え不要）
# ==========================================
WEBHOOK_URL = f"https://{MY_DOMAIN}/api/update_ais.php"
GET_MMSI_URL = f"https://{MY_DOMAIN}/api/get_tracking_mmsi.php"

# グローバル変数
current_tracking_mmsis = [] 
needs_resubscribe = False   
ship_cache = {} 

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
    def log_message(self, format, *args):
        # 邪魔なWebサーバーのアクセスログを消す
        pass

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
    global needs_resubscribe, ship_cache
    
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
                    print(f"📡 【全世界・ピンポイント監視】{len(current_tracking_mmsis)}隻の追跡を開始します。対象MMSI: {current_tracking_mmsis}")
                    subscription_message = {
                        "APIKey": AIS_API_KEY,
                        "BoundingBoxes": [[[-90, -180], [90, 180]]],
                        "FiltersShipMMSI": current_tracking_mmsis
                    }
                
                await websocket.send(json.dumps(subscription_message))
                needs_resubscribe = False

                async for message_json in websocket:
                    if needs_resubscribe:
                        print(f"📡 MMSIリストが更新されたので、新しい注文を送信します。対象MMSI: {current_tracking_mmsis}")
                        new_subscription_message = {
                            "APIKey": AIS_API_KEY,
                            "BoundingBoxes": [[[-90, -180], [90, 180]]], 
                            "FiltersShipMMSI": current_tracking_mmsis
                        }
                        await websocket.send(json.dumps(new_subscription_message))
                        needs_resubscribe = False
                    
                    message = json.loads(message_json)
                    msg_type = message.get("MessageType")
                    
                    if msg_type in ["PositionReport", "ShipStaticData"]:
                        meta = message.get("MetaData", {})
                        mmsi = meta.get("MMSI")
                        ship_name = meta.get("ShipName", "不明").strip()
                        
                        if current_tracking_mmsis and str(mmsi) not in current_tracking_mmsis:
                            continue
                            
                        if str(mmsi) not in ship_cache:
                            ship_cache[str(mmsi)] = {
                                "lat": None, "lon": None, "cog": None, "sog": None, 
                                "status": "不明", "dest": "", "eta": ""
                            }
                            
                        cache = ship_cache[str(mmsi)]
                        
                        if msg_type == "PositionReport":
                            report = message["Message"]["PositionReport"]
                            cache["lat"] = report.get("Latitude")
                            cache["lon"] = report.get("Longitude")
                            cache["cog"] = report.get("Cog")
                            cache["sog"] = report.get("Sog")
                            cache["status"] = report.get("NavigationalStatus", "不明")
                            
                        elif msg_type == "ShipStaticData":
                            static = message.get("Message", {}).get("ShipStaticData", {})
                            
                            dest_raw = static.get("Destination")
                            if dest_raw: 
                                cache["dest"] = str(dest_raw).strip().replace("@", "")
                            
                            eta_data = static.get("Eta")
                            if isinstance(eta_data, dict):
                                month = eta_data.get("Month") or 0
                                day = eta_data.get("Day") or 0
                                hour = eta_data.get("Hour") or 0
                                minute = eta_data.get("Minute") or 0
                                if month > 0 and day > 0:
                                    cache["eta"] = f"{month:02d}/{day:02d} {hour:02d}:{minute:02d}"
                        
                        if cache["lat"] is not None:
                            print(f"🚢 [{mmsi}] {ship_name} 更新 ({msg_type}) -> 転送中...")
                            
                            payload = {
                                "secret": WEBHOOK_SECRET,
                                "mmsi": mmsi,
                                "lat": cache["lat"],
                                "lon": cache["lon"],
                                "cog": cache["cog"],
                                "sog": cache["sog"],
                                "status": cache["status"],
                                "dest": cache["dest"],
                                "eta": cache["eta"]
                            }
                            
                            try:
                                response = requests.post(WEBHOOK_URL, json=payload, timeout=5)
                                if response.status_code == 200:
                                    print(f" 🚀 送信成功: Lat:{cache['lat']}, Dest:{cache['dest']}, ETA:{cache['eta']}")
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
