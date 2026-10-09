import os
import sys
import json
from telethon.sync import TelegramClient

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

# python login.py            -> asosiy "sniper" seansi (avvalgidek)
# python login.py acc2       -> qo'shimcha akkaunt, havzaga avtomatik qo'shiladi
session = sys.argv[1] if len(sys.argv) > 1 else "sniper"

# Telefon raqam, SMS/Telegram kodi va (bo'lsa) 2 bosqichli parol so'raladi
with TelegramClient(session, API_ID, API_HASH) as client:
    me = client.get_me()
    print("Kirildi:", me.first_name)

if session != "sniper":
    owner = int(os.environ["OWNER_ID"])
    try:
        with open("accounts.json") as f:
            data = json.load(f)
    except Exception:
        data = {}
    data[session] = {"owner": owner, "label": me.first_name or session, "claimed": None}
    with open("accounts.json", "w") as f:
        json.dump(data, f)
    print("accounts.json ga qo'shildi. Botni qayta ishga tushiring.")
