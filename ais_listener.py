import asyncio
import websockets
import json
import requests
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# ==========================================
# 1. 設定エリア
# ==========================================
# ★ここにAisStreamのAPIキーを入力してください
AIS_API_KEY = "ここにAisStreamのAPIキー"
WEBHOOK_URL = "https://fair-winds.official.jp/api/update_ais.php"
WEBHOOK_SECRET = "fair_winds_secret_2026"

# ==========================================
# 2. ダミーのWebサーバー（Renderの無料枠を騙すための表の顔）
# ==========================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"AIS Worker is running!")

def run_dummy_server():
    # Renderが指定するポート（無ければ10000）で待機
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), DummyHandler)
    print(f"✅ ダミーWebサーバー起動 (Port: {port})")
    server.serve_forever()

# ==========================================
# 3. AISデータ受信とサーバー転送のメイン処理（裏の顔）
# ==========================================
async def connect_ais_stream():
    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                print("✅ AisStreamに接続しました。東京湾のデータを受信待機中...")
                subscribe_message = {
                    "APIKey": AIS_API_KEY,
                    "BoundingBoxes": [[[34.8, 139.5], [35.7, 140.2]]]
                }
                await websocket.send(json.dumps(subscribe_message))

                async for message_json in websocket:
                    message = json.loads(message_json)
                    if "error" in message:
                        continue
                    if "MetaData" not in message or "MMSI" not in message["MetaData"]:
                        continue

                    mmsi = str(message["MetaData"]["MMSI"])
                    msg_type = message["MessageType"]
                    payload = {"secret": WEBHOOK_SECRET, "mmsi": mmsi}

                    if msg_type == "PositionReport":
                        report = message["Message"]["PositionReport"]
                        payload["lat"] = report["Latitude"]
                        payload["lon"] = report["Longitude"]
                        payload["status"] = get_nav_status_string(report.get("NavigationalStatus", 15))
                        print(f"🚢 [{mmsi}] 位置情報を受信！ => 転送中...")
                    elif msg_type == "ShipStaticData":
                        static_data = message["Message"]["ShipStaticData"]
                        payload["dest"] = static_data.get("Destination", "").strip()
                        eta_month = static_data.get('Eta_month', 0)
                        if eta_month > 0:
                            payload["eta"] = f"{eta_month}/{static_data.get('Eta_day', 0)} {str(static_data.get('Eta_hour', 0)).zfill(2)}:{str(static_data.get('Eta_minute', 0)).zfill(2)}"
                        print(f"🚢 [{mmsi}] 目的地を受信！ => 転送中...")
                    else:
                        continue

                    try:
                        requests.post(WEBHOOK_URL, json=payload, timeout=5)
                    except:
                        pass
                        
        except Exception as e:
            print("⚠️ 接続エラー。3秒後に再接続します...")
            await asyncio.sleep(3)

def get_nav_status_string(status_id):
    statuses = {0: "Under way using engine (航行中)", 1: "At anchor (錨泊中)", 5: "Moored (係留中)"}
    return statuses.get(status_id, f"Unknown ({status_id})")

if __name__ == "__main__":
    # 表のWebサーバーを別スレッドで立ち上げる
    server_thread = threading.Thread(target=run_dummy_server)
    server_thread.daemon = True
    server_thread.start()
    
    # 裏でAIS受信を走らせる
    asyncio.run(connect_ais_stream())