import time
import psycopg2
import requests
from dotenv import load_dotenv
import os
from datetime import timedelta, datetime, timezone, time as dtime
import threading
import pytz

load_dotenv()

# ===================================
# ENV
# ===================================

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Токен бота, который отвечает за брони
TOKEN = os.getenv("TELEGRAM_BOOKING_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")

CHAT_IDS_RAW = (
    os.getenv("TELEGRAM_BOOKING_CHAT_IDS")
    or os.getenv("TELEGRAM_CHAT_IDS")
    or os.getenv("TELEGRAM_CHAT_ID")
    or ""
)
CHAT_IDS = []
for part in CHAT_IDS_RAW.replace(" ", "").split(","):
    if part:
        try:
            CHAT_IDS.append(int(part))
        except ValueError:
            pass

if not CHAT_IDS:
    raise RuntimeError("Не указаны TELEGRAM_BOOKING_CHAT_IDS/TELEGRAM_CHAT_IDS/TELEGRAM_CHAT_ID в .env")

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

last_request_mark = None   # для contract_requests
last_contract_mark = None  # для contracts

# для ежедневной сводки
last_summary_date = None   # дата (в Алматы) за которую уже отправляли отчёт


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
    # в БД времена в UTC → +5 часов до Алматы
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


def now_utc():
    return datetime.now(timezone.utc)


ALMATY_TZ = pytz.timezone("Asia/Almaty")


def to_almaty_dt(dt):
    if not dt:
        return None
    return dt.astimezone(ALMATY_TZ)


def today_almaty():
    return datetime.now(ALMATY_TZ).date()


def yesterday_almaty():
    return today_almaty() - timedelta(days=1)


def daily_report():
    try:
        today = today_almaty()
        yesterday = yesterday_almaty()

        # ---------- 1) БРОНИРОВАНИЯ ЗА ВЧЕРА ----------
        cur.execute("""
            SELECT id, cost, "arrivalDate", "departureDate", "baseApartmentAdData",
                   "tenantInformation", "landlordInformation", "apartmentAdId", "payedAt"
            FROM contracts
            WHERE status = 'CONCLUDED'
              AND "isPaymentSuccess" = true
              AND "payedAt" IS NOT NULL
        """)
        rows = cur.fetchall()

        bookings_yesterday = []
        for row in rows:
            (_, cost, arr, dep, ad, tenant_info, landlord_info, ap_id, payed_at) = row
            if to_almaty_dt(payed_at).date() == yesterday:
                bookings_yesterday.append(row)

        # ---------- 2) ЗАЕЗДЫ СЕГОДНЯ ----------
        cur.execute("""
            SELECT id, cost, "arrivalDate", "departureDate", "baseApartmentAdData",
                   "tenantInformation", "landlordInformation", "apartmentAdId"
            FROM contracts
            WHERE status = 'CONCLUDED'
              AND "isPaymentSuccess" = true
        """)
        rows2 = cur.fetchall()

        arrivals_today = []
        for row in rows2:
            (_, cost, arr, dep, ad, tenant_info, landlord_info, ap_id) = row
            if arr and to_almaty_dt(arr).date() == today:
                arrivals_today.append(row)

        # ---------- 3) ВЫПЛАТЫ СЕГОДНЯ ----------
        payouts_today = []
        total_payout = 0

        for row in rows2:
            (cid, cost, arr, dep, ad, tenant_info, landlord_info, ap_id) = row
            if arr and to_almaty_dt(arr).date() + timedelta(days=1) == today:
                # сумма контракта в тенге (без 1.12)
                contract_sum = round(cost / 100)          # <<< сумма контракта
                payout_sum = round(contract_sum * 0.97)   # <<< минус 3% для владельца
                payouts_today.append((row, payout_sum))
                total_payout += payout_sum

        # ---------- ФОРМИРОВАНИЕ СООБЩЕНИЯ ----------

        msg = f"📊 <b>Ежедневная сводка за {yesterday.strftime('%d.%m.%Y')}</b>\n\n"

        msg += f"📌 <b>Бронирований за вчера:</b> {len(bookings_yesterday)}\n\n"

        msg += "🏨 <b>Предстоящие заезды сегодня:</b>\n"
        if arrivals_today:
            for idx, row in enumerate(arrivals_today, 1):
                (cid, cost, arr, dep, ad, tenant_info, landlord_info, ap_id) = row
                ad_title = (ad or {}).get("title", "Квартира")
                city = (ad or {}).get("address", {}).get("city", "")

                tenant = extract_person(tenant_info)
                landlord = extract_person(landlord_info)
                price = format_price(cost)  # тут гостевая цена, как и раньше
                link = get_apartment_link(ap_id)
                link_line = f'\n      🔗 <a href="{link}">Открыть объявление</a>' if link else ""

                msg += (
                    f"{idx}) <b>{ad_title}</b> — {city}\n"
                    f"   👤 Гость: <b>{tenant['name']}</b>  | 📞 {tenant['phone']}\n"
                    f"   🏡 Собственник: <b>{landlord['name']}</b>  | 📞 {landlord['phone']}\n"
                    f"   📅 Даты: {fmt_date(arr)} → {fmt_date(dep)}\n"
                    f"   💰 Цена: <b>{price:,} ₸</b>{link_line}\n\n"
                )
        else:
            msg += "— нет заездов сегодня\n\n"

        msg += "💵 <b>Выплаты сегодня:</b>\n"
        if payouts_today:
            for idx, (row, payout_sum) in enumerate(payouts_today, 1):
                (_, cost, arr, dep, ad, tenant_info, landlord_info, ap_id) = row
                ad_title = (ad or {}).get("title", "Квартира")
                city = (ad or {}).get("address", {}).get("city", "")

                landlord = extract_person(landlord_info)  # <<< добавили владельца

                msg += (
                    f"{idx}) <b>{ad_title}</b> — {city}\n"
                    f"   🏡 Собственник: <b>{landlord['name']}</b>  | 📞 {landlord['phone']}\n"
                    f"   Сумма: <b>{payout_sum:,} ₸</b>\n"
                )
            msg += f"\n💰 <b>Итого выплат:</b> {total_payout:,} ₸\n"
        else:
            msg += "— сегодня выплат нет\n"

        send(msg)

    except Exception as e:
        print("Daily report error:", e)


def now_almaty():
    # Алматы = UTC+5
    return now_utc() + timedelta(hours=5)


def schedule_daily_report():
    while True:
        now = datetime.now(ALMATY_TZ)
        target = now.replace(hour=9, minute=0, second=0, microsecond=0)

        if now > target:
            target += timedelta(days=1)

        sleep_sec = (target - now).total_seconds()
        time.sleep(sleep_sec)

        daily_report()


# Запускаем отдельным фоном
threading.Thread(target=schedule_daily_report, daemon=True).start()

print("Booking notifier started...")


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
    # 2) contracts (оплаченные / активные / завершённые)
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
            c."updatedAt",
            c."isPaymentSuccess",
            c."payedAt",
            c."retryPaymentAttempts"
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
            c_updated,
            c_is_payment_success,
            c_payed_at,
            c_retry_payment_attempts,
        ) = contract

        c_retry_payment_attempts = c_retry_payment_attempts or 0

        # флаг: пора ли уже считать проживание завершённым по времени
        completed_ready = int(
            c_status == "COMPLETED"
            and c_departure is not None
            and now_utc() >= c_departure
        )

        # учитываем статус, факт оплаты, количество попыток и то, прошёл ли departureDate
        current_mark = (
            f"{c_id}:"
            f"{c_status}:"
            f"{int(bool(c_is_payment_success))}:"
            f"{int(bool(c_payed_at))}:"
            f"{int(c_retry_payment_attempts)}:"
            f"{completed_ready}"
        )

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
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

            elif c_status == "CONCLUDED":
                if c_is_payment_success and c_payed_at:
                    # успешная оплата
                    send(f"""
💳 <b>Бронь оплачена</b>
🕒 Создано: <b>{to_almaty(c_created)}</b>
🕒 Оплачено: <b>{to_almaty(c_payed_at)}</b>

🏠 {title}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>
📞 {landlord['phone']}

📅 {fmt_date(c_arrival)} → {fmt_date(c_departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

                elif (not c_is_payment_success) and c_retry_payment_attempts == 0:
                    # первая автопопытка списания сразу после принятия не удалась
                    send(f"""
💥 <b>Оплата не прошла</b>
Первая попытка списания после принятия заявки закончилась неуспешно.

🏠 {title}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>
📞 {landlord['phone']}

📅 {fmt_date(c_arrival)} → {fmt_date(c_departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

                elif (not c_is_payment_success) and c_retry_payment_attempts >= 1:
                    # повторные попытки оплаты тоже не удались
                    send(f"""
💥 <b>Повторная оплата не прошла</b>
Попыток оплаты: <b>{c_retry_payment_attempts}</b>

🏠 {title}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>
📞 {landlord['phone']}

📅 {fmt_date(c_arrival)} → {fmt_date(c_departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")
                # если статус CONCLUDED, но ни успеха, ни ошибки — ничего не шлём

            elif c_status == "COMPLETED":
                # отправляем только когда по времени уже можно
                if completed_ready:
                    send(f"""
🏁 <b>Проживание завершено</b>
🕒 {to_almaty(c_updated)}

🏠 {title}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>
📞 {landlord['phone']}{link_line}

📅 {fmt_date(c_arrival)} → {fmt_date(c_departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

            elif c_status == "REJECTED":
                # финальный кейс: контракт не состоится
                send(f"""
❌ <b>Контракт отменён</b>
🕒 {to_almaty(c_updated)}

🏠 {title}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>
📞 {landlord['phone']}

📅 {fmt_date(c_arrival)} → {fmt_date(c_departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

            elif c_status == "FREEZE":
                send(f"""
🧊 <b>Контракт заморожен</b>
🕒 {to_almaty(c_updated)}

ID: {c_id}

🏠 {title}{link_line}
🌆 {city}

👤 Гость: <b>{tenant['name']}</b>
📞 {tenant['phone']}

🏡 Собственник: <b>{landlord['name']}</b>
📞 {landlord['phone']}

📅 {fmt_date(c_arrival)} → {fmt_date(c_departure)}
💰 Цена: <b>{price:,} ₸</b>{link_line}
""")

            # в конце обновляем маркер
            last_contract_mark = current_mark

    time.sleep(CHECK_INTERVAL)