#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera eventos dummy e-commerce en JSONL (1 JSON por línea) con un esquema
más cargado de información de negocio.

Uso recomendado para Kafka:
  python generate_events.py --n 200 --stdout | kafka-console-producer ...

Si NO usas --stdout, escribe a archivo (default: events.jsonl).
"""

import argparse
import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

# --- Config dummy ---
CATEGORIES = ["Electrónica", "Ropa", "Hogar", "Deportes", "Belleza", "Libros", "Juguetes"]
BRANDS = ["Acme", "Nova", "Rex", "Lumen", "Orion", "Zenith", "Astra"]
DEVICES = ["mobile", "desktop", "tablet"]
OS_MOBILE = ["iOS", "Android"]
OS_DESKTOP = ["Windows", "macOS", "Linux"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
CURRENCIES = ["USD"]

FIRST_NAMES = [
    "Ana",
    "Luis",
    "Carla",
    "Jorge",
    "María",
    "Diego",
    "Sofía",
    "Pedro",
    "Lucía",
    "Andrés",
]
LAST_NAMES = [
    "García",
    "Martínez",
    "López",
    "Hernández",
    "Pérez",
    "Gómez",
    "Sánchez",
    "Ramírez",
    "Flores",
    "Castillo",
]
SEGMENTS = ["new", "returning", "vip", "wholesale"]
LOYALTY_TIERS = ["none", "silver", "gold", "platinum"]

CHANNELS = ["direct", "organic", "paid", "email", "social", "referral", "affiliate"]
CAMPAIGN_NAMES = [
    "Spring Sale",
    "Weekend Boost",
    "VIP Early Access",
    "Cart Recovery",
    "New Arrivals",
]
CAMPAIGN_TERMS = ["running shoes", "smartwatch", "winter jacket", "skincare", "gaming mouse"]
CAMPAIGN_CONTENT = ["banner_a", "banner_b", "video_15s", "story", "email_a"]

COUPONS = [None, None, None, "SAVE10", "VIP15", "FREESHIP"]
PAYMENT_METHODS = ["card", "paypal", "bank_transfer", "cash_on_delivery"]
PAYMENT_PROVIDERS = ["visa", "mastercard", "amex", "paypal", "stripe"]
ORDER_STATUS = ["paid", "pending", "refunded"]

GEO_LOCATIONS = [
    {
        "country": "Guatemala",
        "region": "Guatemala",
        "city": "Guatemala City",
        "lat": 14.62,
        "lon": -90.52,
        "timezone": "America/Guatemala",
        "tax_rate": 0.12,
        "postal_code": "01001",
    },
    {
        "country": "México",
        "region": "CDMX",
        "city": "Ciudad de México",
        "lat": 19.43,
        "lon": -99.13,
        "timezone": "America/Mexico_City",
        "tax_rate": 0.16,
        "postal_code": "06000",
    },
    {
        "country": "Colombia",
        "region": "Antioquia",
        "city": "Medellín",
        "lat": 6.25,
        "lon": -75.57,
        "timezone": "America/Bogota",
        "tax_rate": 0.19,
        "postal_code": "050021",
    },
    {
        "country": "Chile",
        "region": "RM",
        "city": "Santiago",
        "lat": -33.45,
        "lon": -70.66,
        "timezone": "America/Santiago",
        "tax_rate": 0.19,
        "postal_code": "8320000",
    },
]


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    brand: str
    price: float
    currency: str


@dataclass(frozen=True)
class UserProfile:
    user_id: str
    customer_id: str
    email: str
    first_name: str
    last_name: str
    phone: str
    segment: str
    loyalty_tier: str
    is_member: bool
    signup_date: str


def iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def random_phone(rng: random.Random) -> str:
    return f"+502 5{rng.randint(100,999)} {rng.randint(1000,9999)}"


def random_ip(rng: random.Random) -> str:
    return "{}.{}.{}.{}".format(
        rng.randint(10, 240), rng.randint(0, 255), rng.randint(0, 255), rng.randint(1, 254)
    )


def make_users(n: int, rng: random.Random) -> List[UserProfile]:
    users: List[UserProfile] = []
    for i in range(1, n + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        segment = rng.choice(SEGMENTS)
        loyalty = rng.choice(LOYALTY_TIERS)
        is_member = loyalty != "none"
        signup_days = rng.randint(10, 1200)
        signup_date = (datetime.now(timezone.utc) - timedelta(days=signup_days)).date().isoformat()
        users.append(
            UserProfile(
                user_id=f"U{i:05d}",
                customer_id=f"C{i:05d}",
                email=f"{first.lower()}.{last.lower()}{i}@example.com",
                first_name=first,
                last_name=last,
                phone=random_phone(rng),
                segment=segment,
                loyalty_tier=loyalty,
                is_member=is_member,
                signup_date=signup_date,
            )
        )
    return users


def make_products(n: int, rng: random.Random) -> List[Product]:
    products: List[Product] = []
    for i in range(1, n + 1):
        pid = f"SKU{i:05d}"
        cat = rng.choice(CATEGORIES)
        brand = rng.choice(BRANDS)
        lo, hi = {
            "Electrónica": (40, 1200),
            "Ropa": (8, 150),
            "Hogar": (10, 400),
            "Deportes": (12, 500),
            "Belleza": (5, 120),
            "Libros": (4, 60),
            "Juguetes": (6, 200),
        }[cat]
        price = round(rng.uniform(lo, hi), 2)
        products.append(Product(pid, f"Producto {i:05d}", cat, brand, price, "USD"))
    return products


def weighted_choice(rng: random.Random, choices: List[Tuple[str, float]]) -> str:
    total = sum(w for _, w in choices)
    r = rng.uniform(0, total)
    upto = 0.0
    for item, w in choices:
        upto += w
        if upto >= r:
            return item
    return choices[-1][0]


def cart_totals(cart: Dict[str, int], product_map: Dict[str, Product]) -> Tuple[int, float]:
    qty = sum(cart.values())
    value = sum(product_map[pid].price * q for pid, q in cart.items())
    return qty, round(value, 2)


def pick_cart_item(rng: random.Random, cart: Dict[str, int]) -> Optional[str]:
    if not cart:
        return None
    return rng.choice(list(cart.keys()))


def make_device(rng: random.Random) -> Tuple[dict, str]:
    device_type = rng.choice(DEVICES)
    if device_type == "mobile":
        os_name = rng.choice(OS_MOBILE)
        os_version = f"{rng.randint(12, 17)}.{rng.randint(0, 5)}"
        app_version = f"{rng.randint(2, 5)}.{rng.randint(0, 9)}.{rng.randint(0, 9)}"
        browser = rng.choice(["Chrome", "Safari"])
        event_source = "app" if rng.random() < 0.6 else "web"
    else:
        os_name = rng.choice(OS_DESKTOP)
        os_version = f"{rng.randint(10, 14)}.{rng.randint(0, 9)}"
        app_version = None
        browser = rng.choice(BROWSERS)
        event_source = "web"

    user_agent = f"{browser}/{rng.randint(90,120)}.0 ({os_name} {os_version})"
    return (
        {
            "device_type": device_type,
            "os": os_name,
            "os_version": os_version,
            "app_version": app_version,
            "browser": browser,
            "user_agent": user_agent,
        },
        event_source,
    )


def make_geo(rng: random.Random) -> dict:
    loc = rng.choice(GEO_LOCATIONS)
    return {
        "country": loc["country"],
        "region": loc["region"],
        "city": loc["city"],
        "lat": loc["lat"],
        "lon": loc["lon"],
        "timezone": loc["timezone"],
        "ip": random_ip(rng),
    }


def tax_rate_for_geo(geo: dict) -> float:
    for loc in GEO_LOCATIONS:
        if loc["country"] == geo.get("country") and loc["city"] == geo.get("city"):
            return loc["tax_rate"]
    return 0.12


def postal_code_for_geo(geo: dict) -> str:
    for loc in GEO_LOCATIONS:
        if loc["country"] == geo.get("country") and loc["city"] == geo.get("city"):
            return loc["postal_code"]
    return "00000"


def make_marketing(rng: random.Random) -> dict:
    channel = rng.choice(CHANNELS)
    if channel == "direct":
        return {
            "channel": channel,
            "campaign_id": None,
            "campaign_name": None,
            "medium": "none",
            "source": "direct",
            "term": None,
            "content": None,
            "referrer": None,
        }

    campaign_name = rng.choice(CAMPAIGN_NAMES)
    return {
        "channel": channel,
        "campaign_id": f"CMP{rng.randint(100,999)}",
        "campaign_name": campaign_name,
        "medium": rng.choice(["cpc", "email", "social", "referral"]),
        "source": rng.choice(["google", "facebook", "newsletter", "partner"]),
        "term": rng.choice(CAMPAIGN_TERMS),
        "content": rng.choice(CAMPAIGN_CONTENT),
        "referrer": rng.choice(
            ["https://google.com/", "https://facebook.com/", "https://newsletter.example.com/"]
        ),
    }


def make_page(event_name: str, product_id: Optional[str], order_id: Optional[str], checkout_id: Optional[str]) -> dict:
    if event_name in ("product_view", "click") and product_id:
        url = f"https://example.com/product/{product_id}"
        title = f"Producto {product_id}"
    elif event_name in ("add_to_cart", "update_cart", "remove_from_cart"):
        url = "https://example.com/cart"
        title = "Carrito"
    elif event_name in ("begin_checkout", "checkout_progress"):
        url = f"https://example.com/checkout/{checkout_id or ''}"
        title = "Checkout"
    elif event_name == "purchase" and order_id:
        url = f"https://example.com/order/{order_id}/confirmation"
        title = "Confirmacion de compra"
    else:
        url = "https://example.com/"
        title = "Home"

    path = "/" + "/".join(url.split("/", 3)[-1:]) if url.count("/") >= 3 else "/"
    return {"url": url, "path": path, "title": title}


def make_address(geo: dict, rng: random.Random) -> dict:
    return {
        "country": geo.get("country"),
        "region": geo.get("region"),
        "city": geo.get("city"),
        "postal_code": postal_code_for_geo(geo),
        "address_line1": f"Calle {rng.randint(1, 80)} # {rng.randint(1, 999)}",
    }


def make_cart_snapshot(
    cart: Dict[str, int],
    product_map: Dict[str, Product],
    coupon_code: Optional[str],
    tax_rate: float,
    shipping_cost: float,
) -> dict:
    items_qty, subtotal = cart_totals(cart, product_map)
    discount = 0.0
    if coupon_code == "SAVE10":
        discount = round(subtotal * 0.10, 2)
    elif coupon_code == "VIP15":
        discount = round(subtotal * 0.15, 2)
    elif coupon_code == "FREESHIP":
        shipping_cost = 0.0

    taxable = max(subtotal - discount, 0.0)
    tax = round(taxable * tax_rate, 2)
    value = round(taxable + tax + shipping_cost, 2)

    return {
        "cart_id": None,
        "items_qty": items_qty,
        "value": value,
        "coupon_code": coupon_code,
        "discount": discount,
        "tax": tax,
        "shipping_cost": shipping_cost,
    }


def make_order(
    cart: Dict[str, int],
    product_map: Dict[str, Product],
    coupon_code: Optional[str],
    tax_rate: float,
    shipping_cost: float,
    geo: dict,
    rng: random.Random,
) -> Tuple[dict, List[dict], float]:
    items = []
    subtotal = 0.0
    for pid, q in cart.items():
        p = product_map[pid]
        line_total = round(p.price * q, 2)
        items.append(
            {
                "product_id": pid,
                "name": p.name,
                "category": p.category,
                "brand": p.brand,
                "price": p.price,
                "quantity": q,
                "line_total": line_total,
            }
        )
        subtotal += line_total

    discount = 0.0
    if coupon_code == "SAVE10":
        discount = round(subtotal * 0.10, 2)
    elif coupon_code == "VIP15":
        discount = round(subtotal * 0.15, 2)
    elif coupon_code == "FREESHIP":
        shipping_cost = 0.0

    taxable = max(subtotal - discount, 0.0)
    tax = round(taxable * tax_rate, 2)
    total = round(taxable + tax + shipping_cost, 2)

    order = {
        "order_id": uuid.uuid4().hex,
        "status": rng.choice(ORDER_STATUS),
        "payment_method": rng.choice(PAYMENT_METHODS),
        "payment_provider": rng.choice(PAYMENT_PROVIDERS),
        "installments": rng.choice([1, 1, 1, 3, 6]),
        "subtotal": round(subtotal, 2),
        "discount": discount,
        "tax": tax,
        "shipping_cost": shipping_cost,
        "total": total,
        "currency": "USD",
        "shipping_address": make_address(geo, rng),
        "billing_address": make_address(geo, rng),
    }

    return order, items, total


def base_event(
    *,
    event_name: str,
    ts: datetime,
    user: UserProfile,
    session_id: str,
    device: dict,
    geo: dict,
    marketing: dict,
    event_source: str,
) -> dict:
    # IMPORTANTE: incluye SIEMPRE todas las keys del esquema
    return {
        "event_id": uuid.uuid4().hex,
        "event_time": iso_utc(ts),
        "event_name": event_name,
        "event_version": 2,
        "event_source": event_source,
        "user_id": user.user_id,
        "session_id": session_id,
        "currency": "USD",
        "customer": {
            "customer_id": user.customer_id,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "phone": user.phone,
            "segment": user.segment,
            "loyalty_tier": user.loyalty_tier,
            "is_member": user.is_member,
            "signup_date": user.signup_date,
        },
        "device": device,
        "geo": geo,
        "marketing": marketing,
        "page": None,
        "product_id": None,
        "product": None,
        "quantity": None,
        "price": None,
        "cart": None,
        "checkout_id": None,
        "order": None,
        "items": None,
        "revenue": None,
    }


def generate_events(
    n_events: int = 1000,
    seed: int = 42,
    n_users: int = 200,
    n_products: int = 120,
    days_back: int = 14,
) -> List[dict]:
    rng = random.Random(seed)

    users = make_users(n_users, rng)
    products = make_products(n_products, rng)
    product_map = {p.product_id: p for p in products}

    now = datetime.now(timezone.utc)
    start_base = now - timedelta(days=days_back)

    def random_start_time() -> datetime:
        seconds = rng.randint(0, int((now - start_base).total_seconds()))
        return start_base + timedelta(seconds=seconds)

    events: List[dict] = []

    while len(events) < n_events:
        user = rng.choice(users)
        session_id = uuid.uuid4().hex
        device, event_source = make_device(rng)
        geo = make_geo(rng)
        marketing = make_marketing(rng)

        cart_id = uuid.uuid4().hex
        cart: Dict[str, int] = {}
        in_checkout = False
        checkout_id: Optional[str] = None
        last_product_id: Optional[str] = None
        coupon_code = rng.choice(COUPONS)
        tax_rate = tax_rate_for_geo(geo)
        shipping_cost = round(rng.uniform(2.5, 9.0), 2)

        ts = random_start_time()
        session_target = rng.randint(8, 35)

        for _ in range(session_target):
            if len(events) >= n_events:
                break

            # Probabilidades adaptadas al estado de la sesión
            choices: List[Tuple[str, float]] = [
                ("product_view", 0.36),
                ("click", 0.20 if last_product_id else 0.10),
                ("add_to_cart", 0.16 if last_product_id else 0.06),
            ]
            if cart:
                choices += [
                    ("update_cart", 0.06),
                    ("remove_from_cart", 0.06),
                    ("begin_checkout", 0.06 if not in_checkout else 0.0),
                ]
            if in_checkout:
                choices += [
                    ("checkout_progress", 0.07),
                    ("purchase", 0.05),
                ]
            choices.append(("end_session", 0.02))

            event_name = weighted_choice(rng, choices)
            if event_name == "end_session":
                break

            e = base_event(
                event_name=event_name,
                ts=ts,
                user=user,
                session_id=session_id,
                device=device,
                geo=geo,
                marketing=marketing,
                event_source=event_source,
            )

            if event_name == "product_view":
                p = rng.choice(products)
                last_product_id = p.product_id
                e["product_id"] = p.product_id
                e["product"] = {
                    "product_id": p.product_id,
                    "name": p.name,
                    "category": p.category,
                    "brand": p.brand,
                    "price": p.price,
                    "currency": p.currency,
                }
                e["price"] = p.price

            elif event_name == "click":
                pid = last_product_id or rng.choice(products).product_id
                p = product_map[pid]
                last_product_id = pid
                e["product_id"] = pid
                e["product"] = {
                    "product_id": p.product_id,
                    "name": p.name,
                    "category": p.category,
                    "brand": p.brand,
                    "price": p.price,
                    "currency": p.currency,
                }
                e["price"] = p.price

            elif event_name == "add_to_cart":
                pid = last_product_id or rng.choice(products).product_id
                p = product_map[pid]
                qty_add = rng.choice([1, 1, 1, 2, 2, 3])
                cart[pid] = cart.get(pid, 0) + qty_add
                e["product_id"] = pid
                e["product"] = {
                    "product_id": p.product_id,
                    "name": p.name,
                    "category": p.category,
                    "brand": p.brand,
                    "price": p.price,
                    "currency": p.currency,
                }
                e["quantity"] = qty_add
                e["price"] = p.price

                cart_snapshot = make_cart_snapshot(
                    cart, product_map, coupon_code, tax_rate, shipping_cost
                )
                cart_snapshot["cart_id"] = cart_id
                e["cart"] = cart_snapshot

            elif event_name == "update_cart":
                pid = pick_cart_item(rng, cart)
                if pid is None:
                    continue
                p = product_map[pid]
                new_qty = rng.randint(1, 5)
                cart[pid] = new_qty
                e["product_id"] = pid
                e["product"] = {
                    "product_id": p.product_id,
                    "name": p.name,
                    "category": p.category,
                    "brand": p.brand,
                    "price": p.price,
                    "currency": p.currency,
                }
                e["quantity"] = new_qty
                e["price"] = p.price

                cart_snapshot = make_cart_snapshot(
                    cart, product_map, coupon_code, tax_rate, shipping_cost
                )
                cart_snapshot["cart_id"] = cart_id
                e["cart"] = cart_snapshot

            elif event_name == "remove_from_cart":
                pid = pick_cart_item(rng, cart)
                if pid is None:
                    continue
                p = product_map[pid]
                removed_qty = cart.get(pid, 1)
                cart.pop(pid, None)
                e["product_id"] = pid
                e["product"] = {
                    "product_id": p.product_id,
                    "name": p.name,
                    "category": p.category,
                    "brand": p.brand,
                    "price": p.price,
                    "currency": p.currency,
                }
                e["quantity"] = removed_qty
                e["price"] = p.price

                cart_snapshot = make_cart_snapshot(
                    cart, product_map, coupon_code, tax_rate, shipping_cost
                )
                cart_snapshot["cart_id"] = cart_id
                e["cart"] = cart_snapshot

            elif event_name == "begin_checkout":
                if not cart or in_checkout:
                    continue
                in_checkout = True
                checkout_id = uuid.uuid4().hex
                e["checkout_id"] = checkout_id

                cart_snapshot = make_cart_snapshot(
                    cart, product_map, coupon_code, tax_rate, shipping_cost
                )
                cart_snapshot["cart_id"] = cart_id
                e["cart"] = cart_snapshot

            elif event_name == "checkout_progress":
                if not in_checkout or not checkout_id:
                    continue
                e["checkout_id"] = checkout_id

                cart_snapshot = make_cart_snapshot(
                    cart, product_map, coupon_code, tax_rate, shipping_cost
                )
                cart_snapshot["cart_id"] = cart_id
                e["cart"] = cart_snapshot

            elif event_name == "purchase":
                if not in_checkout or not checkout_id or not cart:
                    continue

                order, items, revenue = make_order(
                    cart,
                    product_map,
                    coupon_code,
                    tax_rate,
                    shipping_cost,
                    geo,
                    rng,
                )

                e["checkout_id"] = checkout_id
                e["order"] = order
                e["items"] = items
                e["revenue"] = revenue

                cart.clear()
                in_checkout = False
                checkout_id = None

            e["page"] = make_page(event_name, e.get("product_id"), None if e.get("order") is None else e["order"].get("order_id"), e.get("checkout_id"))

            events.append(e)
            ts += timedelta(seconds=rng.randint(5, 140))

    return events


def write_jsonl(path: str, events: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Genera eventos dummy e-commerce (JSONL) con estructura cargada.")
    ap.add_argument("--n", type=int, default=1000, help="Cantidad total de eventos (default: 1000)")
    ap.add_argument("--seed", type=int, default=42, help="Semilla RNG (default: 42)")
    ap.add_argument("--users", type=int, default=200, help="Cantidad de usuarios (default: 200)")
    ap.add_argument("--products", type=int, default=120, help="Cantidad de productos (default: 120)")
    ap.add_argument("--days-back", type=int, default=14, help="Ventana temporal hacia atrás (default: 14 días)")
    ap.add_argument("--out", default="events.jsonl", help="Archivo de salida JSONL (default: events.jsonl)")
    ap.add_argument("--stdout", action="store_true", help="Imprimir eventos a stdout (JSON por línea)")
    args = ap.parse_args()

    events = generate_events(
        n_events=args.n,
        seed=args.seed,
        n_users=args.users,
        n_products=args.products,
        days_back=args.days_back,
    )

    if args.stdout:
        for e in events:
            print(json.dumps(e, ensure_ascii=False))
    else:
        write_jsonl(args.out, events)
        print(f"OK: {len(events)} eventos -> {args.out}")


if __name__ == "__main__":
    main()
