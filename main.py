import os, re, json, time, asyncio, logging
import httpx
from datetime import datetime, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes,
)
from telethon import TelegramClient, errors
from telethon.tl import types as T
from telethon.tl.functions.channels import (
    CreateChannelRequest, UpdateUsernameRequest, DeleteChannelRequest,
)
from telethon.tl.functions.account import (
    CheckUsernameRequest as AccCheck,
    UpdateUsernameRequest as AccUpdate,
)

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger("sniper")

TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])

CHECK_DELAY = 3     # soniya: tekshiruvlar orasida (kamaytirmang!)
MAX_CHECK = 200     # bir martada nechta username tekshirilsin
VERIFY_DELAY = 2        # Telegramdan 'bo'shmi?' deb so'rash orasidagi pauza (soniya)
MAX_VERIFY = 10         # bir xabarda ko'pi bilan nechta username Telegramdan tasdiqlanadi
RESOLVE_DELAY = 3       # akkaunt qidirish orasidagi pauza (soniya)
MAX_RESOLVE = 25        # bir xabarda ko'pi bilan nechta akkaunt qidiriladi
INACTIVE_DAYS = 180  # shuncha kundan ko'p kirmagan akkaunt avtomatik kuzatuvga qo'shiladi
MAX_WATCH = 100     # bir vaqtda ko'pi bilan nechta username
WATCH_FILE = "watch.json"
CFG_FILE = "config.json"
USERS_FILE = "users.json"      # {approved:{id:label}, pending:{id:label}, blocked:{id:vaqt}}
ACC_FILE = "accounts.json"     # {session: {owner, label, claimed}}
MAX_ACC_USER = 3               # oddiy foydalanuvchi ko'pi bilan nechta akkaunt qo'sha oladi
REJECT_COOLDOWN = 24 * 3600    # rad etilgan foydalanuvchi qayta so'rashi uchun kutish
AUTH_MODES = ("api_id", "api_hash", "phone", "code", "password")

VALID = re.compile(r"^[a-z][a-z0-9_]{3,30}[a-z0-9]$")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 13) Chrome/120 Mobile",
    "Accept-Language": "en-US,en;q=0.9",
}
STATUS_RE = re.compile(
    r'tm-section-header-status\s+tm-status-\w+"[^>]*>\s*([^<]+?)\s*<'
)
sem = asyncio.Semaphore(3)


# ---------- fayllar ----------
def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def write_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f)


watch = read_json(WATCH_FILE)   # {username: kuzatuvchi_user_id}
cfg = read_json(CFG_FILE)       # {api_id, api_hash}
users = read_json(USERS_FILE)
users.setdefault("approved", {})
users.setdefault("pending", {})
users.setdefault("blocked", {})

# eski format (qiymat 0) -> egasiga biriktiramiz
for _k, _v in list(watch.items()):
    if not _v:
        watch[_k] = OWNER_ID


def save():
    write_json(WATCH_FILE, watch)


def save_users():
    write_json(USERS_FILE, users)


def count_watch(uid):
    return sum(1 for v in watch.values() if v == uid)


# ---------- akkauntlar havzasi (pool) ----------
class Acc:
    def __init__(self, session, owner, label=""):
        self.session = session
        self.owner = owner
        self.label = label
        self.claimed = None
        self.tg_id = None
        self.client = None
        self.auth = False
        self.block = {"check": 0.0, "resolve": 0.0}   # FloodWait tugash vaqti
        self.last = {"check": 0.0, "resolve": 0.0}    # oxirgi so'rov vaqti


accs = {}   # session -> Acc


def save_accs():
    write_json(ACC_FILE, {
        s: {"owner": a.owner, "label": a.label, "claimed": a.claimed}
        for s, a in accs.items()
    })


def load_accs():
    for s, m in read_json(ACC_FILE).items():
        a = Acc(s, int(m.get("owner", OWNER_ID)), m.get("label", ""))
        a.claimed = m.get("claimed")
        accs[s] = a
    # login.py yaratgan asosiy seans
    if "sniper" not in accs and os.path.exists("sniper.session"):
        a = Acc("sniper", OWNER_ID, "asosiy")
        a.claimed = cfg.get("acct_claimed")
        accs["sniper"] = a
    save_accs()


def live(uid=None):
    return [a for a in accs.values()
            if a.auth and a.client and (uid is None or a.owner == uid)]


def pool_wait(kind):
    """Barcha akkauntlar bloklangan bo'lsa, eng yaqin ochilishgacha soniya; aks holda 0."""
    ls = live()
    if not ls:
        return 0
    now = time.time()
    if any(a.block[kind] <= now for a in ls):
        return 0
    return min(a.block[kind] for a in ls) - now


async def acquire(kind, delay):
    """Bloklanmagan va eng kam ishlatilgan akkauntni qaytaradi (so'rovlar orasida delay saqlanadi).
    Hammasi bloklangan bo'lsa None."""
    while True:
        now = time.time()
        cands = [a for a in live() if a.block[kind] <= now]
        if not cands:
            return None
        a = min(cands, key=lambda x: x.last[kind])
        slot = max(now, a.last[kind] + delay)
        a.last[kind] = slot
        if slot > now:
            await asyncio.sleep(slot - now)
        if a.auth and a.block[kind] <= time.time():
            return a


class PoolBlocked(Exception):
    def __init__(self, seconds):
        self.seconds = seconds


DEAD_ERRORS = (errors.AuthKeyUnregisteredError, errors.SessionRevokedError)


def get_creds():
    try:
        return int(cfg["api_id"]), str(cfg["api_hash"])
    except Exception:
        pass
    try:
        return int(os.environ["API_ID"]), os.environ["API_HASH"]
    except Exception:
        return None


async def connect_acc(a):
    creds = get_creds()
    if not creds:
        return False
    if a.client is None:
        a.client = TelegramClient(a.session, *creds)
    if not a.client.is_connected():
        await a.client.connect()
    a.auth = await a.client.is_user_authorized()
    if a.auth and a.tg_id is None:
        me = await a.client.get_me()
        a.tg_id = me.id
    return a.auth


async def remove_acc(session):
    a = accs.pop(session, None)
    if not a:
        return
    a.auth = False
    if a.client:
        try:
            await a.client.log_out()
        except Exception:
            try:
                await a.client.disconnect()
            except Exception:
                pass
    try:
        os.remove(session + ".session")
    except Exception:
        pass
    save_accs()


async def dead(a):
    """Seansi yopilgan akkauntni havzadan olib tashlaydi va egalarini xabardor qiladi."""
    label = a.label or a.session
    await remove_acc(a.session)
    text = f"⚠️ {label} seansi yopilgan, akkaunt olib tashlandi. 🔑 orqali qayta qo'shing."
    await notify(app, a.owner, text)
    if a.owner != OWNER_ID:
        await notify(app, OWNER_ID, text)


# ---------- kuzatuv ----------
def parse_names(text):
    names = []
    # faqat @ bilan boshlangan so'zlar (email ichidagi @ hisobga olinmaydi)
    for n in re.findall(r"(?<![A-Za-z0-9_])@([A-Za-z0-9_]+)", text):
        n = n.lower()
        if n not in names:
            names.append(n)
    return names


class AccountBusy(Exception):
    pass


last_warn = {}


async def add_watch(name, uid):
    watch[name] = uid
    save()


async def drop(name):
    watch.pop(name, None)
    save()


async def notify(app, uid, text):
    try:
        await app.bot.send_message(uid, text)
    except Exception as e:
        log.warning("xabar yuborilmadi: %s", e)


async def warn(app, uid, key, text, every=600):
    """Bir xil ogohlantirishni 10 daqiqada bir martadan ko'p yubormaydi."""
    if time.time() - last_warn.get(key, 0) > every:
        last_warn[key] = time.time()
        await notify(app, uid, text)


async def claim_with(a, name):
    """Username bo'shaganda kanal ochib o'rnatadi.
    Kanal ochish cheklangan bo'lsa, akkaunt usernamesiga o'rnatadi."""
    c = a.client
    try:
        res = await c(CreateChannelRequest(title=name, about="", broadcast=True))
    except errors.UserRestrictedError:
        if a.claimed:
            raise AccountBusy()
        await c(AccUpdate(name))
        a.claimed = name
        save_accs()
        return "akkaunt usernamesi"
    inp = await c.get_input_entity(res.chats[0])
    try:
        await c(UpdateUsernameRequest(inp, name))
    except Exception:
        try:
            await c(DeleteChannelRequest(inp))
        except Exception:
            pass
        raise
    return "kanal"


async def claim(name, uid):
    """Egallashni kuzatuvchining O'Z akkauntida bajaradi (bo'lmasa egasining akkauntida)."""
    mine = live(uid) or live(OWNER_ID) or live()
    err = None
    for a in mine:
        try:
            return await claim_with(a, name)
        except AccountBusy as e:
            err = e
    raise err or AccountBusy()


async def sniper_loop(app):
    while True:
        if not live() or not watch:
            await asyncio.sleep(3)
            continue
        w = pool_wait("check")
        if w > 0:
            await asyncio.sleep(min(w + 1, 60))
            continue
        for name in list(watch):
            if name not in watch:
                continue
            a = await acquire("check", CHECK_DELAY)
            if a is None:
                break
            uid = watch.get(name, OWNER_ID)
            try:
                if await a.client(AccCheck(name)):
                    where = await claim(name, uid)
                    await drop(name)
                    await notify(app, uid, f"🎉 @{name} egallandi ({where})!\nt.me/{name}")
            except errors.FloodWaitError as e:
                log.warning("FloodWait (%s): %s soniya", a.session, e.seconds)
                a.block["check"] = time.time() + e.seconds
                continue   # boshqa akkaunt bilan davom etamiz
            except DEAD_ERRORS:
                await dead(a)
                continue
            except AccountBusy:
                await warn(
                    app, uid, name,
                    f"⚠️ @{name} BO'SH, lekin kanal ochish cheklangan va "
                    "akkaunt usernamesi allaqachon band. Tezroq o'zingiz oling!",
                )
            except Exception as e:
                msg = str(e).upper()
                if "OCCUPIED" in msg or "PURCHASE" in msg:
                    pass  # hali band, Fragment'da sotuvda yoki boshqa birov ulgurdi
                elif "INVALID" in msg:
                    await drop(name)
                    await notify(app, uid, f"❌ @{name} yaroqsiz, kuzatuvdan olindi.")
                elif "PUBLIC_TOO_MUCH" in msg:
                    await warn(
                        app, uid, "limit",
                        f"⚠️ @{name} BO'SH, lekin ommaviy kanallar limiti to'lgan. "
                        "Eski kanallardan birini yopiq qiling yoki o'chiring.",
                    )
                else:
                    log.warning("@%s: %s", name, e)


# ---------- username tekshiruvchi ----------
async def fragment_status(client, name):
    f = await client.get(
        f"https://fragment.com/username/{name}",
        follow_redirects=True,
    )
    m = STATUS_RE.search(f.text)
    return m.group(1).strip() if m else ""


async def tg_available(name):
    """Telegramning o'zidan so'raydi (account.checkUsername), havzadagi akkauntlar navbat bilan.
    ('free'|'taken'|'sale'|'invalid'|'unknown', izoh)"""
    for _ in range(max(1, len(accs))):
        a = await acquire("check", VERIFY_DELAY)
        if a is None:
            break
        try:
            ok = await a.client(AccCheck(name))
            return ("free", "") if ok else ("taken", "")
        except errors.FloodWaitError as e:
            a.block["check"] = time.time() + e.seconds
            continue
        except DEAD_ERRORS:
            await dead(a)
            continue
        except Exception as e:
            m = str(e).upper()
            if "PURCHASE" in m:
                return "sale", ""
            if "OCCUPIED" in m:
                return "taken", ""
            if "INVALID" in m:
                return "invalid", ""
            return "unknown", type(e).__name__
    return "unknown", f"Telegram tekshiruvni yana {fmt_wait(pool_wait('check'))} cheklagan"


async def check(client, name, verify=None):
    if not VALID.match(name) or "__" in name:
        return f"❌ @{name} — format yaroqsiz"
    async with sem:
        try:
            st = await fragment_status(client, name)
            s = st.lower()

            if s and "unavailable" not in s:
                if "sold" in s:
                    return f"🔒 @{name} — NFT, sotilgan"
                if "avail" in s or "auction" in s or "sale" in s:
                    return f"💎 @{name} — Fragment'da sotuvda ({st})"

            r = await client.get(f"https://t.me/{name}")
            if "tgme_page_title" in r.text:
                return f"👤 @{name} — band (akkaunt bor)"

            if s and "unavailable" not in s and "taken" not in s:
                return f"❓ @{name} — Fragment holati: {st}"

            if verify is not None:
                if verify["left"] <= 0:
                    return (f"❔ @{name} — ehtimol bo'sh (bir xabarda faqat {MAX_VERIFY} tasi "
                            "Telegramdan tasdiqlanadi)")
                verify["left"] -= 1
                st2, info = await tg_available(name)
                if st2 == "taken":
                    return f"🚫 @{name} — Telegram bo'sh demaydi (band, ban yoki zaxirada)"
                if st2 == "invalid":
                    return f"🚫 @{name} — Telegram yaroqsiz deydi (ban yoki zaxirada)"
                if st2 == "sale":
                    return f"💎 @{name} — Fragment'da sotuvda (Telegram: sotib olish mumkin)"
                if st2 == "unknown":
                    return f"❔ @{name} — ehtimol bo'sh, Telegram tasdiqlamadi ({info})"
                return f"✅ @{name} — BO'SH (Telegram tasdiqladi)"
            return f"❔ @{name} — ehtimol bo'sh (Telegram bilan tasdiqlanmadi)"
        except Exception as e:
            return f"⚠️ @{name} — xato: {type(e).__name__}"
        finally:
            await asyncio.sleep(0.5)


# ---------- akkauntga kirish (bot ichida) ----------
async def wipe(msg):
    """Maxfiy ma'lumot yozilgan xabarni chatdan o'chiradi."""
    try:
        await msg.delete()
    except Exception:
        pass


def new_session(uid):
    return f"s{uid}_{int(time.time())}"


async def drop_login(ud):
    """Tugallanmagan kirishni tozalaydi."""
    lc = ud.pop("lc", None)
    sess = ud.pop("lsess", None)
    if lc:
        try:
            await lc.disconnect()
        except Exception:
            pass
    if sess and sess not in accs:
        try:
            os.remove(sess + ".session")
        except Exception:
            pass
    for k in ("phone", "hash"):
        ud.pop(k, None)


async def begin_login(msg, ctx, uid):
    mine = [a for a in accs.values() if a.owner == uid]
    if uid != OWNER_ID and len(mine) >= MAX_ACC_USER:
        await msg.reply_text(f"Siz ko'pi bilan {MAX_ACC_USER} ta akkaunt qo'sha olasiz.")
        return
    if not get_creds():
        if uid != OWNER_ID:
            await msg.reply_text("Bot egasi hali API ma'lumotlarini kiritmagan. Keyinroq urinib ko'ring.")
            return
        ctx.user_data["mode"] = "api_id"
        await msg.reply_text(
            "my.telegram.org → API development tools bo'limidan olgan "
            "api_id raqamingizni yuboring."
        )
        return
    ctx.user_data["mode"] = "phone"
    note = ""
    if uid != OWNER_ID:
        note = (
            "ℹ️ Akkauntingiz bot serverida saqlanadi (bot egasi serverga kira oladi), "
            "umumiy tekshiruvlarda ham ishlatiladi, sizning kuzatuvlaringiz esa aynan shu "
            "akkauntga egallanadi. Istalgan payt 📱 Akkauntlar orqali o'chira olasiz.\n\n"
        )
    await msg.reply_text(note + "Telefon raqamingizni yuboring (masalan +998901234567).")


async def finish_login(msg, ctx, uid):
    ud = ctx.user_data
    lc = ud.pop("lc")
    session = ud.pop("lsess")
    for k in ("phone", "hash", "mode"):
        ud.pop(k, None)
    me = await lc.get_me()
    if any(a.tg_id == me.id for a in accs.values()):
        try:
            await lc.log_out()
        except Exception:
            pass
        try:
            os.remove(session + ".session")
        except Exception:
            pass
        await msg.reply_text("Bu akkaunt allaqachon qo'shilgan.", reply_markup=menu(uid == OWNER_ID))
        return
    label = (me.first_name or "").strip()
    if me.username:
        label += f" @{me.username}"
    a = Acc(session, uid, label or session)
    a.client, a.auth, a.tg_id = lc, True, me.id
    accs[session] = a
    save_accs()
    await msg.reply_text(
        f"✅ Ulandi: {a.label}\n"
        "Telegram → Sozlamalar → Qurilmalar bo'limida yangi seans ko'rinadi.\n"
        f"Havzada jami {len(live())} ta akkaunt.",
        reply_markup=menu(uid == OWNER_ID),
    )


async def auth_step(update: Update, ctx: ContextTypes.DEFAULT_TYPE, mode):
    msg = update.message
    text = msg.text.strip()
    ud = ctx.user_data
    uid = update.effective_user.id

    if mode in ("api_id", "api_hash") and uid != OWNER_ID:
        ud.pop("mode", None)
        return

    if mode == "api_id":
        if not text.isdigit():
            await msg.reply_text("api_id faqat raqamlardan iborat. Qayta yuboring.")
            return
        ud["api_id"] = int(text)
        ud["mode"] = "api_hash"
        await msg.reply_text("Endi api_hash ni yuboring (32 ta harf-raqam).")

    elif mode == "api_hash":
        await wipe(msg)
        if not re.fullmatch(r"[0-9a-fA-F]{32}", text) or "api_id" not in ud:
            await msg.reply_text(
                "api_hash 32 ta harf-raqamdan iborat. Qayta yuboring."
            )
            return
        cfg["api_id"] = ud.pop("api_id")
        cfg["api_hash"] = text
        write_json(CFG_FILE, cfg)
        ud["mode"] = "phone"
        await msg.reply_text(
            "Saqlandi ✅\nEndi telefon raqamingizni yuboring (+998901234567)."
        )

    elif mode == "phone":
        await wipe(msg)
        phone = "+" + re.sub(r"\D", "", text)
        creds = get_creds()
        if not creds:
            ud.pop("mode", None)
            await msg.reply_text("API ma'lumotlari yo'q.")
            return
        await drop_login(ud)
        session = new_session(uid)
        client = TelegramClient(session, *creds)
        ud["lc"], ud["lsess"] = client, session
        try:
            await client.connect()
            sent = await client.send_code_request(phone)
        except errors.PhoneNumberInvalidError:
            await drop_login(ud)
            await msg.reply_text("Raqam noto'g'ri. +998901234567 shaklida yuboring.")
            return
        except errors.ApiIdInvalidError:
            await drop_login(ud)
            cfg.clear()
            write_json(CFG_FILE, cfg)
            if uid == OWNER_ID:
                ud["mode"] = "api_id"
                await msg.reply_text(
                    "api_id/api_hash noto'g'ri ekan. api_id ni qayta yuboring."
                )
            else:
                ud.pop("mode", None)
                await msg.reply_text("Botning API ma'lumotlari noto'g'ri. Egasiga xabar bering.")
            return
        except errors.FloodWaitError as e:
            await drop_login(ud)
            await msg.reply_text(
                f"Juda ko'p urinish. {e.seconds} soniyadan keyin qayta urining."
            )
            return
        except Exception as e:
            await drop_login(ud)
            await msg.reply_text(f"Xato: {type(e).__name__}")
            return
        ud.update(phone=phone, hash=sent.phone_code_hash, mode="code")
        await msg.reply_text(
            "Kod yuborildi ✅\n\n"
            "Kodni raqamlar orasiga bo'sh joy qo'yib yuboring, masalan:\n"
            "1 2 3 4 5\n\n"
            "(Oddiy yozsangiz, Telegram kodni bloklab qo'yadi.)"
        )

    elif mode == "code":
        await wipe(msg)
        lc = ud.get("lc")
        if lc is None:
            ud["mode"] = "phone"
            await msg.reply_text("Seans eskirdi. Telefon raqamni qayta yuboring.")
            return
        code = re.sub(r"\D", "", text)
        try:
            await lc.sign_in(ud["phone"], code, phone_code_hash=ud["hash"])
        except errors.SessionPasswordNeededError:
            ud["mode"] = "password"
            await msg.reply_text("2 bosqichli parolni yuboring.")
            return
        except errors.PhoneCodeInvalidError:
            await msg.reply_text("Kod noto'g'ri. Qayta yuboring (1 2 3 4 5).")
            return
        except errors.PhoneCodeExpiredError:
            await drop_login(ud)
            ud["mode"] = "phone"
            await msg.reply_text(
                "Kod eskirdi yoki bloklandi. Telefon raqamni qayta yuboring, "
                "kodni esa bo'sh joy bilan yozing."
            )
            return
        except Exception as e:
            await msg.reply_text(f"Xato: {type(e).__name__}")
            return
        await finish_login(msg, ctx, uid)

    elif mode == "password":
        await wipe(msg)
        lc = ud.get("lc")
        if lc is None:
            ud["mode"] = "phone"
            await msg.reply_text("Seans eskirdi. Telefon raqamni qayta yuboring.")
            return
        try:
            await lc.sign_in(password=text)
        except errors.PasswordHashInvalidError:
            await msg.reply_text("Parol noto'g'ri. Qayta yuboring.")
            return
        except Exception as e:
            await msg.reply_text(f"Xato: {type(e).__name__}")
            return
        await finish_login(msg, ctx, uid)


# ---------- bot ----------
def is_owner(update: Update):
    u = update.effective_user
    return bool(u and u.id == OWNER_ID)


def is_allowed(uid):
    return uid == OWNER_ID or str(uid) in users["approved"]


def label_of(u):
    s = u.full_name or str(u.id)
    if u.username:
        s += f" (@{u.username})"
    return s


def menu(owner=False):
    rows = [
        [InlineKeyboardButton("🔑 Akkaunt qo'shish", callback_data="login")],
        [InlineKeyboardButton("🎯 Kuzatuvga qo'shish", callback_data="add")],
        [
            InlineKeyboardButton("📋 Ro'yxat", callback_data="list"),
            InlineKeyboardButton("🗑 Tozalash", callback_data="clear"),
        ],
        [InlineKeyboardButton("📱 Akkauntlar", callback_data="accs")],
    ]
    if owner:
        rows.append([InlineKeyboardButton("👥 Foydalanuvchilar", callback_data="users")])
    return InlineKeyboardMarkup(rows)


inactive_cache = {}   # username -> (vaqt, uzoq_kirmagan, izoh)


def fmt_wait(sec):
    sec = int(sec)
    if sec < 60:
        return f"{sec} soniya"
    h, m = sec // 3600, (sec % 3600) // 60
    return f"{h} soat {m} daqiqa" if h else f"{m} daqiqa"


async def pool_entity(name):
    """Username bo'yicha entity: havzadagi akkauntlar navbat bilan, FloodWait bo'lsa keyingisi."""
    for _ in range(max(1, len(accs))):
        a = await acquire("resolve", RESOLVE_DELAY)
        if a is None:
            break
        try:
            return await a.client.get_entity(name)
        except errors.FloodWaitError as e:
            a.block["resolve"] = time.time() + e.seconds
        except DEAD_ERRORS:
            await dead(a)
    raise PoolBlocked(pool_wait("resolve"))


async def inactive_info(name):
    """(uzoq_kirmagan, izoh). Faqat oddiy akkauntlar uchun; bot/kanal -> False.
    Telegram username qidirishni qattiq cheklaydi, shuning uchun natija keshlanadi."""
    hit = inactive_cache.get(name)
    if hit and time.time() - hit[0] < 6 * 3600:
        return hit[1], hit[2]
    e = await pool_entity(name)
    res = (False, "")
    if isinstance(e, T.User) and not e.bot and not e.deleted:
        st = e.status
        if st is None or isinstance(st, T.UserStatusEmpty):
            res = (True, "uzoq vaqt oldin faol bo'lgan (kamida 1 oy)")
        elif isinstance(st, T.UserStatusOffline):
            wo = st.was_online
            if wo.tzinfo is None:
                wo = wo.replace(tzinfo=timezone.utc)
            days = (datetime.now(timezone.utc) - wo).days
            if days >= INACTIVE_DAYS:
                res = (True, f"oxirgi faollik: {wo:%Y-%m-%d}, {days} kun oldin")
    inactive_cache[name] = (time.time(), *res)
    return res


async def describe(name):
    """Username kimniki ekanini va oxirgi faolligini qisqa matnda qaytaradi."""
    try:
        e = await pool_entity(name)
    except (ValueError, errors.UsernameNotOccupiedError, errors.UsernameInvalidError):
        return "hozir akkaunt topilmadi"
    except Exception:
        return "holat aniqlanmadi"
    if isinstance(e, T.User):
        if e.bot:
            return "bot (inaktivlik bilan o'chmaydi)"
        s = e.status
        if isinstance(s, T.UserStatusOnline):
            return "akkaunt, hozir onlayn"
        if isinstance(s, T.UserStatusOffline):
            return f"akkaunt, oxirgi faollik: {s.was_online:%Y-%m-%d}"
        if isinstance(s, T.UserStatusRecently):
            return "akkaunt, yaqinda faol (yoki yashirilgan)"
        if isinstance(s, T.UserStatusLastWeek):
            return "akkaunt, 1 hafta ichida faol"
        if isinstance(s, T.UserStatusLastMonth):
            return "akkaunt, 1 oy ichida faol"
        return "akkaunt, uzoq vaqt oldin faol bo'lgan"
    return "kanal/guruh (inaktivlik bilan o'chmaydi)"


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if is_allowed(u.id):
        await update.message.reply_text(
            "Matn ichida @ bilan boshlangan usernamelarni yuboring (masalan @durov @test), "
            "bir martada 200 tagacha).",
            reply_markup=menu(u.id == OWNER_ID),
        )
        return
    key = str(u.id)
    ts = users["blocked"].get(key)
    if ts and time.time() - ts < REJECT_COOLDOWN:
        return   # yaqinda rad etilgan: jim
    if key in users["pending"]:
        await update.message.reply_text("⏳ So'rovingiz bot egasiga yuborilgan. Javobni kuting.")
        return
    users["blocked"].pop(key, None)
    users["pending"][key] = label_of(u)
    save_users()
    await update.message.reply_text(
        "🔒 Bu bot yopiq. Bot egasiga ruxsat so'rovi yuborildi — tasdiqlansa, xabar keladi."
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Ruxsat berish", callback_data=f"ok:{u.id}"),
        InlineKeyboardButton("❌ Rad etish", callback_data=f"no:{u.id}"),
    ]])
    await notify_kb(ctx.application, OWNER_ID, f"🆕 Ruxsat so'ralmoqda:\n{label_of(u)}\nID: {u.id}", kb)


async def notify_kb(app, uid, text, kb):
    try:
        await app.bot.send_message(uid, text, reply_markup=kb)
    except Exception as e:
        log.warning("xabar yuborilmadi: %s", e)


def acc_lines(uid):
    """(matn, tugmalar) — egasi hammasini, boshqalar faqat o'zinikini ko'radi."""
    items = [a for a in accs.values() if uid == OWNER_ID or a.owner == uid]
    if not items:
        return "Akkaunt yo'q. 🔑 orqali qo'shing.", []
    lines, btns = [], []
    now = time.time()
    for a in items:
        st = "✅" if a.auth else "⚠️ ulanmagan"
        w = max(a.block["check"] - now, 0)
        extra = f" ⏳ {fmt_wait(w)}" if w > 0 else ""
        who = ""
        if uid == OWNER_ID and a.owner != OWNER_ID:
            who = f" — {users['approved'].get(str(a.owner), a.owner)}"
        lines.append(f"{st} {a.label or a.session}{who}{extra}")
        btns.append([InlineKeyboardButton(f"🗑 {a.label or a.session}", callback_data=f"accdel:{a.session}")])
    return f"📱 Akkauntlar ({len(live())} ta faol):\n" + "\n".join(lines), btns


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = q.data or ""

    # --- egasi: ruxsat boshqaruvi ---
    if uid == OWNER_ID:
        if data.startswith(("ok:", "no:")):
            target = data[3:]
            label = users["pending"].pop(target, None)
            if label is None:
                await q.edit_message_text("Bu so'rov allaqachon ko'rib chiqilgan.")
                return
            if data.startswith("ok:"):
                users["approved"][target] = label
                save_users()
                await q.edit_message_text(f"✅ Ruxsat berildi: {label}")
                await notify(ctx.application, int(target), "✅ Ruxsat berildi! /start ni bosing.")
            else:
                users["blocked"][target] = time.time()
                save_users()
                await q.edit_message_text(f"❌ Rad etildi: {label}")
                await notify(ctx.application, int(target), "❌ So'rovingiz rad etildi.")
            return
        if data == "users":
            if not users["approved"]:
                await q.message.reply_text("Ruxsat berilgan foydalanuvchilar yo'q.")
                return
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🚫 {lbl}", callback_data=f"rm:{k}")]
                for k, lbl in users["approved"].items()
            ])
            await q.message.reply_text(
                "👥 Ruxsat berilgan foydalanuvchilar.\n"
                "Tugmani bossangiz ruxsati, kuzatuvlari va akkauntlari o'chiriladi:",
                reply_markup=kb,
            )
            return
        if data.startswith("rm:"):
            target = data[3:]
            label = users["approved"].pop(target, None)
            save_users()
            if label is None:
                await q.edit_message_text("Allaqachon o'chirilgan.")
                return
            tid = int(target)
            for n in [n for n, v in watch.items() if v == tid]:
                await drop(n)
            for s in [s for s, a in accs.items() if a.owner == tid]:
                await remove_acc(s)
            await q.edit_message_text(f"🚫 Ruxsat olib tashlandi: {label}")
            await notify(ctx.application, tid, "🚫 Botdan foydalanish ruxsatingiz olib tashlandi.")
            return

    if not is_allowed(uid):
        return

    if data == "login":
        await begin_login(q.message, ctx, uid)
    elif data == "add":
        if not live(uid):
            await q.message.reply_text(
                "Avval o'z akkauntingizni qo'shing: 🔑 Akkaunt qo'shish tugmasini bosing."
            )
            return
        ctx.user_data["mode"] = "watch"
        await q.message.reply_text(
            "Egallamoqchi bo'lgan usernamelarni yuboring.\n"
            "Ular bo'shashi bilan kanal ochib egallayman."
        )
    elif data == "list":
        mine = [(n, v) for n, v in watch.items() if uid == OWNER_ID or v == uid]
        if mine:
            out = []
            for n, v in mine:
                tag = ""
                if uid == OWNER_ID and v != OWNER_ID:
                    tag = f"  ({users['approved'].get(str(v), v)})"
                out.append(f"@{n}{tag}")
            await q.message.reply_text("🎯 Kuzatilmoqda:\n" + "\n".join(out))
        else:
            await q.message.reply_text("Ro'yxat bo'sh.")
    elif data == "clear":
        for n in [n for n, v in watch.items() if v == uid]:
            await drop(n)
        await q.message.reply_text("Ro'yxat tozalandi.")
    elif data == "accs":
        text, btns = acc_lines(uid)
        await q.message.reply_text(text, reply_markup=InlineKeyboardMarkup(btns) if btns else None)
    elif data.startswith("accdel:"):
        s = data[7:]
        a = accs.get(s)
        if not a or (uid != OWNER_ID and a.owner != uid):
            await q.message.reply_text("Akkaunt topilmadi.")
            return
        label = a.label or s
        await remove_acc(s)
        await q.message.reply_text(f"🗑 {label} o'chirildi (seans Telegramdan ham chiqarildi).")


async def handle_watch(update: Update, names):
    uid = update.effective_user.id
    if not live(uid):
        await update.message.reply_text("Avval o'z akkauntingizni qo'shing (🔑).")
        return
    lines = []
    for n in names:
        if not VALID.match(n) or "__" in n:
            lines.append(f"❌ @{n} — format yaroqsiz")
        elif n in watch:
            lines.append(f"ℹ️ @{n} — allaqachon kuzatilmoqda")
        elif count_watch(uid) >= MAX_WATCH:
            lines.append(f"⚠️ @{n} — limit ({MAX_WATCH}) to'ldi")
        else:
            try:
                info = await describe(n)
                await add_watch(n, uid)
                lines.append(f"🎯 @{n} — kuzatuvga qo'shildi\n    ↳ {info}")
            except Exception as e:
                lines.append(f"⚠️ @{n} — xato: {type(e).__name__}")
    await update.message.reply_text("\n".join(lines), reply_markup=menu(uid == OWNER_ID))


async def handle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid):
        await update.message.reply_text("🔒 Ruxsat yo'q. /start bosib, bot egasidan ruxsat so'rang.")
        return
    mode = ctx.user_data.get("mode")
    if mode in AUTH_MODES:
        await auth_step(update, ctx, mode)
        return
    if mode == "watch":
        ctx.user_data.pop("mode", None)
        names = parse_names(update.message.text)[:MAX_CHECK]
        if names:
            await handle_watch(update, names)
        return
    names = parse_names(update.message.text)[:MAX_CHECK]
    if not names:
        return
    msg = await update.message.reply_text("⏳ Tekshirilmoqda...")
    sem = asyncio.Semaphore(10)

    n_live = len(live())
    verify_budget = {"left": MAX_VERIFY * n_live} if n_live else None

    async def limited(client, n):
        async with sem:
            return await check(client, n, verify=verify_budget)

    async with httpx.AsyncClient(headers=HEADERS, timeout=15) as client:
        results = await asyncio.gather(*(limited(client, n) for n in names))

    # Uzoq kirmagan akkauntlarni avtomatik kuzatuvga qo'shish (o'z akkaunti bor foydalanuvchi uchun)
    results = list(results)
    inactive = {}   # indeks -> izoh
    notes = []
    added = 0
    has_acc = any(r.startswith("👤") for r in results)
    if has_acc and not live(uid):
        notes.append("ℹ️ Avtomatik kuzatuv uchun avval o'z akkauntingizni qo'shing (🔑).")
    elif has_acc:
        resolved = 0
        max_res = MAX_RESOLVE * n_live
        for i, (n, r) in enumerate(zip(names, results)):
            if not r.startswith("👤"):
                continue
            if n in watch:
                inactive[i] = "allaqachon kuzatuvda"
                continue
            if count_watch(uid) >= MAX_WATCH:
                notes.append(f"⚠️ Kuzatuv limiti ({MAX_WATCH}) to'ldi, qolganlari qo'shilmadi.")
                break
            cached = n in inactive_cache
            if not cached:
                left = pool_wait("resolve")
                if left > 0:
                    notes.append(
                        f"⏳ Telegram akkaunt qidirishni yana {fmt_wait(left)} cheklagan: "
                        "avtomatik kuzatuv vaqtincha to'xtatildi (tekshiruv ishlayveradi)."
                    )
                    break
                if resolved >= max_res:
                    notes.append(
                        f"ℹ️ Bir xabarda {max_res} tadan ortiq akkaunt tekshirilmaydi "
                        "(Telegram limitidan saqlanish uchun). Qolganlarini keyin yuboring."
                    )
                    break
            try:
                old_acc, info = await inactive_info(n)
                if not cached:
                    resolved += 1
                if old_acc:
                    await add_watch(n, uid)
                    added += 1
                    inactive[i] = info
            except (errors.FloodWaitError, PoolBlocked) as e:
                notes.append(
                    f"⏳ Telegram akkaunt qidirishni {fmt_wait(e.seconds)}ga cheklab qo'ydi: "
                    "avtomatik kuzatuv vaqtincha to'xtatildi (tekshiruv ishlayveradi)."
                )
                break
            except Exception:
                pass
    if added:
        notes.append(f"🎯 Jami {added} ta username kuzatuvga qo'shildi.")

    # Natijani guruhlarga ajratamiz (aralashmasin)
    titles = {
        "✅": "✅ BO'SH (Telegram tasdiqlagan)",
        "❔": "❔ EHTIMOL BO'SH (tasdiqlanmagan)",
        "🎯": "🎯 UZOQ KIRMAGAN (kuzatuvda)",
        "💎": "💎 FRAGMENT'DA SOTUVDA",
        "🔒": "🔒 NFT, SOTILGAN",
        "👤": "👤 BAND (faol akkaunt)",
        "🚫": "🚫 EGALLAB BO'LMAYDI (ban/band/zaxira)",
        "❓": "❓ NOANIQ",
        "❌": "❌ YAROQSIZ FORMAT",
        "⚠": "⚠️ XATO",
    }
    groups = {}
    for i, (n, r) in enumerate(zip(names, results)):
        if i in inactive:
            key, r = "🎯", f"🎯 @{n} — {inactive[i]}"
        else:
            key = r[:1]
            if key not in titles:
                key = "❓"
        groups.setdefault(key, []).append(r)

    lines = []
    for key, title in titles.items():
        items = groups.get(key)
        if items:
            if lines:
                lines.append("")
            lines.append(f"{title} — {len(items)} ta")
            lines.extend(items)
    if notes:
        lines.append("")
        lines.extend(notes)

    # Telegram xabar limiti 4096 belgi: natijani qatorlar bo'yicha bo'laklarga bo'lamiz
    chunks, cur = [], ""
    for line in lines:
        if len(cur) + len(line) + 1 > 4000:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur:
        chunks.append(cur)

    await msg.edit_text(chunks[0].rstrip())
    for ch in chunks[1:]:
        await update.message.reply_text(ch.rstrip())


async def post_init(application):
    load_accs()
    for a in list(accs.values()):
        try:
            await connect_acc(a)
        except Exception as e:
            log.warning("%s ulanmadi: %s", a.session, e)
    application.bot_data["sniper"] = asyncio.create_task(sniper_loop(application))
    print("Bot ishga tushdi. Faol akkauntlar:", len(live()), "/", len(accs))


async def post_shutdown(application):
    for a in accs.values():
        if a.client:
            try:
                await a.client.disconnect()
            except Exception:
                pass


app = (
    ApplicationBuilder()
    .token(TOKEN)
    .post_init(post_init)
    .post_shutdown(post_shutdown)
    .build()
)
app.add_handler(CommandHandler("start", start))
app.add_handler(CallbackQueryHandler(on_button))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle))
app.run_polling()
