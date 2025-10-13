import os
import json
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import pytz
from collections import defaultdict

# === Firebase 初期化（GitHub Secrets 経由）===
def initialize_firebase():
    try:
        # GitHub Actions の Secrets から読み込み
        cred_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
        if not cred_json:
            raise ValueError("❌ FIREBASE_SERVICE_ACCOUNT not found in environment variables.")

        cred_dict = json.loads(cred_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        print("✅ Firebase initialized successfully.")
        return firestore.client()
    except Exception as e:
        print(f"❌ Firebase initialization failed: {e}")
        raise e

# Firestore接続
db = initialize_firebase()

# === タイムゾーン設定（米国東部時間）===
ET = pytz.timezone("America/New_York")
now_et = datetime.now(ET)
date_str = now_et.strftime("%Y-%m-%d %H:%M:%S ET")
print(f"🏁 [Rankings] Updating rankings at {date_str}")

# === Firestoreコレクション参照 ===
predictions_ref = db.collection("predictions")
rankings_ref = db.collection("rankings")
users_ref = db.collection("users")

# === 集計用データ構造 ===
stats = defaultdict(lambda: {"correct": 0, "total": 0, "score": 0})

# === 1️⃣ predictions から全データを走査 ===
print("📊 Fetching predictions...")
predictions = predictions_ref.stream()
count = 0

for doc in predictions:
    count += 1
    data = doc.to_dict()
    uid = data.get("uid")
    if not uid:
        continue

    # 投票が確定済み(isCorrect)のみカウント
    is_correct = data.get("isCorrect", None)
    if is_correct is None:
        continue  # 未判定のものはスキップ

    stats[uid]["total"] += 1
    if is_correct:
        stats[uid]["correct"] += 1
        stats[uid]["score"] += 10  # 正解ごとに10pt加算

print(f"✅ Processed {count} prediction records.")

# === 2️⃣ 集計結果を rankings に書き込み ===
updated_users = 0
for uid, s in stats.items():
    accuracy = s["correct"] / s["total"] if s["total"] > 0 else 0.0

    # Firestoreに書き込むデータ
    ranking_data = {
        "score": s["score"],
        "totalVotes": s["total"],
        "correctVotes": s["correct"],
        "accuracy": round(accuracy, 3),
        "lastUpdated": firestore.SERVER_TIMESTAMP,
    }

    # 匿名ID（displayId）が users/{uid} にあれば追加
    user_doc = users_ref.document(uid).get()
    if user_doc.exists:
        user_data = user_doc.to_dict()
        display_id = user_data.get("displayId")
        if display_id:
            ranking_data["displayId"] = display_id

    # rankings/{uid} に反映
    rankings_ref.document(uid).set(ranking_data, merge=True)
    updated_users += 1

print(f"✅ [Rankings] Updated {updated_users} users successfully.")
