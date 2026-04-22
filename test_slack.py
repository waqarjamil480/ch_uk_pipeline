import urllib.request
import json
from dotenv import load_dotenv
import os

load_dotenv()  # loads .env from the same folder

WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not WEBHOOK_URL:
    print("❌ SLACK_WEBHOOK_URL not found in .env")
    exit(1)

payload = {"text": "✅ CH Pipeline Slack test — working!"}

data = json.dumps(payload).encode("utf-8")
req  = urllib.request.Request(
    WEBHOOK_URL,
    data=data,
    headers={"Content-Type": "application/json"},
    method="POST"
)

with urllib.request.urlopen(req) as r:
    response = r.read().decode()
    if response == "ok":
        print("✅ Success — Slack is working! Check your channel.")
    else:
        print(f"⚠️  Unexpected response: {response}")