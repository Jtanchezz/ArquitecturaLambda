#!/usr/bin/env python3
import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone

EVENT_NAMES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "purchase",
]

DEVICES = ["mobile", "desktop", "tablet"]
CURRENCIES = ["USD", "EUR", "MXN", "COP"]
PRODUCTS = [f"sku-{i:04d}" for i in range(1, 301)]
PAGES = [
    "/",
    "/home",
    "/category/electronics",
    "/category/apparel",
    "/cart",
    "/checkout",
]
REFERRERS = [
    None,
    "https://google.com",
    "https://instagram.com",
    "https://tiktok.com",
    "https://newsletter.example.com",
]


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_event(event_time: datetime) -> dict:
    event_name = random.choices(
        EVENT_NAMES,
        weights=[40, 30, 15, 5, 5, 5],
        k=1,
    )[0]

    user_id = f"user-{random.randint(1, 5000):05d}"
    session_id = str(uuid.uuid4().hex)
    device = random.choice(DEVICES)
    currency = random.choice(CURRENCIES)

    product_id = None
    quantity = None
    price = None
    cart_id = str(uuid.uuid4().hex)
    checkout_id = None
    order_id = None
    cart_items_qty = None
    cart_value = None
    revenue = None
    page_url = None
    referrer = None
    items = None

    if event_name in {"product_view", "add_to_cart", "remove_from_cart", "checkout_started", "purchase"}:
        product_id = random.choice(PRODUCTS)
        quantity = random.randint(1, 3)
        price = round(random.uniform(5.0, 250.0), 2)
        cart_items_qty = random.randint(1, 6)
        cart_value = round(cart_items_qty * price, 2)

    if event_name == "page_view":
        page_url = random.choice(PAGES)
        referrer = random.choice(REFERRERS)

    if event_name in {"checkout_started", "purchase"}:
        checkout_id = str(uuid.uuid4().hex)
        items = [
            {
                "product_id": random.choice(PRODUCTS),
                "quantity": random.randint(1, 3),
                "price": round(random.uniform(5.0, 250.0), 2),
            }
            for _ in range(random.randint(1, 4))
        ]
        cart_items_qty = sum(i["quantity"] for i in items)
        cart_value = round(sum(i["quantity"] * i["price"] for i in items), 2)

    if event_name == "purchase":
        order_id = str(uuid.uuid4().hex)
        revenue = cart_value

    return {
        "event_id": uuid.uuid4().hex,
        "event_time": iso_utc(event_time),
        "event_name": event_name,
        "user_id": user_id,
        "session_id": session_id,
        "device": device,
        "currency": currency,
        "product_id": product_id,
        "quantity": quantity,
        "price": price,
        "cart_id": cart_id,
        "checkout_id": checkout_id,
        "order_id": order_id,
        "cart_items_qty": cart_items_qty,
        "cart_value": cart_value,
        "revenue": revenue,
        "page_url": page_url,
        "referrer": referrer,
        "items": items,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate ecommerce events as JSON lines.")
    parser.add_argument("--count", type=int, default=100, help="Number of events to generate")
    parser.add_argument(
        "--start-minutes-ago",
        type=int,
        default=120,
        help="How many minutes in the past to start generating timestamps",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=5,
        help="Seconds between events",
    )
    args = parser.parse_args()

    start = datetime.now(timezone.utc) - timedelta(minutes=args.start_minutes_ago)

    for i in range(args.count):
        event_time = start + timedelta(seconds=i * args.interval_seconds)
        event = make_event(event_time)
        print(json.dumps(event, ensure_ascii=True))


if __name__ == "__main__":
    main()
