import asyncio
import websockets
import json
import requests

# ==========================================
# 1. 設定エリア
# ==========================================
# ★ここにAisStreamのAPIキーを入力してください
AIS_API_KEY = "2ec9dc1fe4dfa685ace12e3e3b23fd2c7582628e"

# レンタルサーバーに作ったPHPの受け取り窓口のURL
WEBHOOK_URL = "https://fair-winds.official.jp/api/update_ais.php"

# セキュリティ用の合言葉（PHP側と一致させる）
WEBHOOK_SECRET = "fair_winds_secret_2026"

# ==========================================
# 2. AISデータ受信とサーバー転送のメイン処理
# ==========================================
async def connect_ais_stream():
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        print("✅ AisStreamに接続しました。東京湾のデータを受信待機中...")

        # 購読リクエストの送信（ドーバー海峡のみ指定、MMSIの絞り込みなし！）
        subscribe_message = {
            "Apikey": AIS_API_KEY,
            "BoundingBoxes": [[[34.8, 139.5], [35.7, 140.2]]]
        }
        await websocket.send(json.dumps(subscribe_message))

        # データを受信し続けるループ
        async for message_json in websocket:
            message = json.loads(message_json)
            
            if "error" in message:
                print(f"❌ エラー: {message['error']}")
                continue

            if "MetaData" not in message or "MMSI" not in message["MetaData"]:
                continue

            mmsi = str(message["MetaData"]["MMSI"])
            msg_type = message["MessageType"]

            # サーバーへ送るデータの準備
            payload = {
                "secret": WEBHOOK_SECRET,
                "mmsi": mmsi
            }

            if msg_type == "PositionReport":
                report = message["Message"]["PositionReport"]
                payload["lat"] = report["Latitude"]
                payload["lon"] = report["Longitude"]
                payload["status"] = get_nav_status_string(report.get("NavigationalStatus", 15))
                print(f"🚢 [{mmsi}] 位置情報を受信！ => サーバーへ転送中...")

            elif msg_type == "ShipStaticData":
                static_data = message["Message"]["ShipStaticData"]
                payload["dest"] = static_data.get("Destination", "").strip()
                eta_month = static_data.get('Eta_month', 0)
                if eta_month > 0:
                    payload["eta"] = f"{eta_month}/{static_data.get('Eta_day', 0)} {str(static_data.get('Eta_hour', 0)).zfill(2)}:{str(static_data.get('Eta_minute', 0)).zfill(2)}"
                print(f"🚢 [{mmsi}] 目的地/ETAを受信！ => サーバーへ転送中...")
            else:
                continue

            # 3. レンタルサーバー(PHP)へデータをPOST送信
            try:
                response = requests.post(WEBHOOK_URL, json=payload)
                print(f" => サーバー応答: {response.text}")
            except Exception as e:
                print(f" => ❌ 転送エラー: {e}")

# 航海ステータスを文字に変換する関数
def get_nav_status_string(status_id):
    statuses = {
        0: "Under way using engine (航行中)", 1: "At anchor (錨泊中)",
        2: "Not under command (運転不自由船)", 3: "Restricted maneuverability (操縦性能制限船)",
        4: "Constrained by draft (喫水制限船)", 5: "Moored (係留中)",
        6: "Aground (乗揚)", 7: "Engaged in fishing (漁労中)", 8: "Under way sailing (帆走中)"
    }
    return statuses.get(status_id, f"Unknown ({status_id})")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())