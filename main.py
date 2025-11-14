import time
import psycopg2
import requests
from dotenv import load_dotenv
import os
from datetime import timedelta

load_dotenv()

# ===================================
# ENV
# ===================================

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# поддержка нескольких chat_id
CHAT_IDS_RAW = os.getenv("TELEGRAM_CHAT_IDS") or os.getenv("TELEGRAM_CHAT_ID") or ""
CHAT_IDS = []
for part in CHAT_IDS_RAW.replace(" ", "").split(","):
    if part:
        try:
            CHAT_IDS.append(int(part))
        except ValueError:
            pass

if not CHAT_IDS:
    raise RuntimeError("Не указаны TELEGRAM_CHAT_IDS или TELEGRAM_CHAT_ID в .env")

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 10))


# ===================================
# Telegram
# ===================================

def send(text: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    for chat_id in CHAT_IDS:
        try:
            requests.post(url, json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            })
        except Exception as e:
            print(f"Telegram error for chat {chat_id}:", e)


# ===================================
# DB Connection
# ===================================

DB_CONN = (
    f"host={DB_HOST} "
    f"port={DB_PORT} "
    f"dbname={DB_NAME} "
    f"user={DB_USER} "
    f"password={DB_PASSWORD}"
)

conn = psycopg2.connect(DB_CONN)
cur = conn.cursor()

# будем отслеживать "id:status", чтобы ловить смену статуса
last_request_mark = None
last_cancel_mark = None
last_contract_mark = None


# ===================================
# Helpers
# ===================================

def fmt_date(d):
    if not d:
        return "-"
    return d.strftime("%d.%m.%Y")

def to_almaty(dt):
    if not dt:
        return "-"
    return (dt + timedelta(hours=5)).strftime("%d.%m.%Y %H:%M")

def format_price(cost):
    # cost / 100 * 1.12
    return round(cost / 100 * 1.12)

def get_user_info(user_id):
    if not user_id:
        return {"name": "—", "phone": "—"}
    try:
        cur.execute("""
            SELECT "firstName", "lastName", phone 
            FROM users 
            WHERE id = %s LIMIT 1;
        """, (user_id,))
        row = cur.fetchone()
        if not row:
            return {"name": "—", "phone": "—"}
        first, last, phone = row
        full_name = f"{first or ''} {last or ''}".strip() or "—"
        return {"name": full_name, "phone": phone or "—"}
    except Exception as e:
        print("get_user_info error:", e)
        return {"name": "—", "phone": "—"}

def extract_person(info_json, fallback_user_id=None):
    """
    info_json: {"firstName","lastName","phoneNumber", ...}
    """
    name = "—"
    phone = "—"

    if isinstance(info_json, dict):
        first = info_json.get("firstName") or ""
        last = info_json.get("lastName") or ""
        full = f"{first} {last}".strip()
        if full:
            name = full
        phone = (
            info_json.get("phoneNumber")
            or info_json.get("phone")
            or "—"
        )

    # если из JSON чего-то не хватает — добиваем из users
    if fallback_user_id and (name == "—" or phone == "—"):
        u = get_user_info(fallback_user_id)
        if name == "—":
            name = u["name"]
        if phone == "—":
            phone = u["phone"]

    return {"name": name, "phone": phone}

def get_apartment_link(apartment_id):
    if not apartment_id:
        return ""
    try:
        cur.execute("""
            SELECT slug
            FROM apartment_identificator
            WHERE "apartmentId" = %s
            ORDER BY "createdAt" DESC
            LIMIT 1;
        """, (apartment_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            return ""
        slug = row[0]
        return f"https://livin.kz/apartment/{slug}"
    except Exception as e:
        print("get_apartment_link error:", e)
        return ""


print("Notifier started...")


# ===================================
# MAIN LOOP
# ===================================

while True:

    # =====================================================
    # 1) contract_requests (заявки)
    # =====================================================
    cur.execute("""
        SELECT 
            r.id,
            r.status,
            r.cost,
            r."arrivalDate",
            r."departureDate",
            r."baseApartmentAdData",
            r."tenantId",
            r."tenantInformation",
            r."landlordInformation",
            r."apartmentAdId",
            r."createdAt",
            r."updatedAt"
        FROM contract_requests r
        ORDER BY r."updatedAt" DESC
        LIMIT 1;
    """)

    req = cur.fetchone()

    if req:
        (
            req_id,
            status,
            cost,
            arrival,
            departure,
            ad_info,
            tenant_id,
            tenant_info_json,
            landlord_info_json,
            apartment_ad_id,
            created_at,
            updated_at
        ) = req

        current_mark = f"{req_id}:{status}"
        if last_request_mark is None:
            last_request_mark = current_mark
        elif current_mark != last_request_mark:

            ad_title = (ad_info or {}).get("title", "Квартира")
            city = (ad_info or {}).get("address", {}).get("city", "")

            tenant = extract_person(tenant_info_json, fallback_user_id=tenant_id)
            landlord = extract_person(landlord_info_json)

            price = format_price(cost)
            link = get_apartment_link(apartment_ad_id)
            link_line = f'\n🔗 <a href="{link}">Открыть объявление</a>' if link else ""

            # PENDING уже нет, работаем с CREATED / ACCEPTED / REJECTED
            if status == "CREATED":
                send(f"""
✉️ <b>Заявка отправлена</b>
🕒 Создано: <b>{to_almaty(created_at)}</b>

👤 Гость: <b>{tenant['name']}</b>  
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>  
📞 {landlord['phone']}

🏠 Квартира: <b>{ad_title}</b>
🌆 {city}

📅 {fmt_date(arrival)} → {fmt_date(departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

            elif status == "ACCEPTED":
                send(f"""
✅ <b>Заявка принята собственником</b>
🕒 Создано: <b>{to_almaty(created_at)}</b>
🕒 Обновлено: <b>{to_almaty(updated_at)}</b>

👤 Гость: <b>{tenant['name']}</b>  
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>  
📞 {landlord['phone']}

🏠 Квартира: <b>{ad_title}</b>
🌆 {city}

📅 {fmt_date(arrival)} → {fmt_date(departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

            elif status == "REJECTED":
                send(f"""
❌ <b>Заявка отклонена</b>
🕒 Создано: <b>{to_almaty(created_at)}</b>
🕒 Обновлено: <b>{to_almaty(updated_at)}</b>

👤 Гость: <b>{tenant['name']}</b>  
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>  
📞 {landlord['phone']}

🏠 Квартира: <b>{ad_title}</b>
🌆 {city}

📅 {fmt_date(arrival)} → {fmt_date(departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

            last_request_mark = current_mark


    # =====================================================
    # 2) contract_cancel_requests (отмена)
    # =====================================================

    cur.execute("""
        SELECT 
            id, 
            "senderRole",
            "rejectReason",
            status,
            "createdAt",
            "updatedAt"
        FROM contract_cancel_requests
        ORDER BY "updatedAt" DESC
        LIMIT 1;
    """)

    cancel = cur.fetchone()

    if cancel:
        cancel_id, role, reason, status, created_at, updated_at = cancel

        current_mark = f"{cancel_id}:{status}"
        if last_cancel_mark is None:
            last_cancel_mark = current_mark
        elif current_mark != last_cancel_mark:

            if status == "PROCESSING":
                send(f"""
⚠️ <b>Запрос на отмену</b>
🕒 Создано: <b>{to_almaty(created_at)}</b>

От: <b>{role}</b>
Причина: {reason}
""")

            elif status == "APPROVED":
                send(f"""
🟢 <b>Отмена одобрена</b>
🕒 Обновлено: <b>{to_almaty(updated_at)}</b>

От: <b>{role}</b>
Причина: {reason}
""")

            elif status == "DECLINED":
                send(f"""
🔴 <b>Отмена отклонена</b>
🕒 Обновлено: <b>{to_almaty(updated_at)}</b>

От: <b>{role}</b>
Причина: {reason}
""")

            last_cancel_mark = current_mark


    # =====================================================
    # 3) contracts (оплаченные / активные / завершённые)
    # =====================================================

    cur.execute("""
        SELECT
            c.id,
            c.status,
            c.cost,
            c."arrivalDate",
            c."departureDate",
            c."baseApartmentAdData",
            c."tenantId",
            c."landlordId",
            c."tenantInformation",
            c."landlordInformation",
            c."apartmentAdId",
            c."createdAt",
            c."updatedAt"
        FROM contracts c
        ORDER BY c."updatedAt" DESC
        LIMIT 1;
    """)

    contract = cur.fetchone()

    if contract:
        (
            c_id,
            c_status,
            c_cost,
            c_arrival,
            c_departure,
            c_ad,
            tenant_id,
            landlord_id,
            c_tenant_info,
            c_landlord_info,
            c_apartment_ad_id,
            c_created,
            c_updated
        ) = contract

        current_mark = f"{c_id}:{c_status}"
        if last_contract_mark is None:
            last_contract_mark = current_mark
        elif current_mark != last_contract_mark:

            # OFFERING не используем
            if c_status == "OFFERING":
                last_contract_mark = current_mark
                time.sleep(CHECK_INTERVAL)
                continue

            tenant = extract_person(c_tenant_info, fallback_user_id=tenant_id)
            landlord = extract_person(c_landlord_info, fallback_user_id=landlord_id)

            title = (c_ad or {}).get("title", "Квартира")
            city = (c_ad or {}).get("address", {}).get("city", "")
            price = format_price(c_cost)
            link = get_apartment_link(c_apartment_ad_id)
            link_line = f'\n🔗 <a href="{link}">Открыть объявление</a>' if link else ""

            if c_status == "CREATED":
                send(f"""
📄 <b>Контракт создан</b>
🕒 {to_almaty(c_created)}

👤 Гость: <b>{tenant['name']}</b>  
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>  
📞 {landlord['phone']}

🏠 {title}
🌆 {city}

📅 {fmt_date(c_arrival)} → {fmt_date(c_departure)}
💰 {price:,} ₸{link_line}
""")

            elif c_status == "CONCLUDED":
                send(f"""
💳 <b>Бронь оплачена</b>
🕒 {to_almaty(c_updated)}

🏠 {title}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>  
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>  
📞 {landlord['phone']}

💰 <b>{price:,} ₸</b>{link_line}
""")

            elif c_status == "COMPLETED":
                send(f"""
🏁 <b>Проживание завершено</b>
🕒 {to_almaty(c_updated)}

🏠 {title}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>  
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>  
📞 {landlord['phone']}{link_line}
""")

            elif c_status == "REJECTED":
                send(f"""
❌ <b>Контракт отменён</b>
🕒 {to_almaty(c_updated)}

🏠 {title}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>  
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>  
📞 {landlord['phone']}{link_line}
""")

            elif c_status == "FREEZE":
                send(f"""
🧊 <b>Контракт заморожен</b>
🕒 {to_almaty(c_updated)}

ID: {c_id}
🏠 {title}{link_line}
""")

            last_contract_mark = current_mark


    time.sleep(CHECK_INTERVAL)