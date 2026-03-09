#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera eventos dummy e-commerce en JSONL (1 JSON por línea) con esta estructura fija.

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
DEVICES = ["mobile", "desktop", "tablet"]
CURRENCIES = ["USD"]
REFERRERS = [
    "https://example.com/",
    "https://google.com/",
    "https://facebook.com/",
    "https://newsletter.example.com/",
    "https://partner.example.com/",
]


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    price: float


def iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_users(n: int) -> List[str]:
    return [f"U{i:05d}" for i in range(1, n + 1)]


def make_products(n: int, rng: random.Random) -> List[Product]:
    products: List[Product] = []
    for i in range(1, n + 1):
        pid = f"SKU{i:05d}"
        cat = rng.choice(CATEGORIES)
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
        products.append(Product(pid, f"Producto {i:05d}", cat, price))
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


def base_event(
    *,
    event_name: str,
    ts: datetime,
    user_id: str,
    session_id: str,
    device: str,
    currency: str,
    cart_id: str,
    referrer: str,
) -> dict:
    # IMPORTANTE: incluye SIEMPRE todas las keys del esquema
    return {
        "event_id": uuid.uuid4().hex,
        "event_time": iso_utc(ts),
        "event_name": event_name,
        "user_id": user_id,
        "session_id": session_id,
        "device": device,
        "currency": currency,
        "product_id": None,
        "quantity": None,
        "price": None,
        "cart_id": cart_id,
        "checkout_id": None,
        "order_id": None,
        "cart_items_qty": None,
        "cart_value": None,
        "revenue": None,
        "page_url": None,
        "referrer": referrer,
        "items": None,
    }


def generate_events(
    n_events: int = 1000,
    seed: int = 42,
    n_users: int = 200,
    n_products: int = 120,
    days_back: int = 14,
) -> List[dict]:
    rng = random.Random(seed)

    users = make_users(n_users)
    products = make_products(n_products, rng)
    product_map = {p.product_id: p for p in products}

    now = datetime.now(timezone.utc)
    start_base = now - timedelta(days=days_back)

    def random_start_time() -> datetime:
        seconds = rng.randint(0, int((now - start_base).total_seconds()))
        return start_base + timedelta(seconds=seconds)

    events: List[dict] = []

    while len(events) < n_events:
        user_id = rng.choice(users)
        session_id = uuid.uuid4().hex
        device = rng.choice(DEVICES)
        currency = rng.choice(CURRENCIES)
        referrer = rng.choice(REFERRERS)

        cart_id = uuid.uuid4().hex
        cart: Dict[str, int] = {}
        in_checkout = False
        checkout_id: Optional[str] = None
        last_product_id: Optional[str] = None

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
                user_id=user_id,
                session_id=session_id,
                device=device,
                currency=currency,
                cart_id=cart_id,
                referrer=referrer,
            )

            if event_name == "product_view":
                p = rng.choice(products)
                last_product_id = p.product_id
                e["product_id"] = p.product_id
                e["price"] = p.price
                e["page_url"] = f"https://example.com/product/{p.product_id}"

            elif event_name == "click":
                pid = last_product_id or rng.choice(products).product_id
                p = product_map[pid]
                last_product_id = pid
                e["product_id"] = pid
                e["price"] = p.price
                e["page_url"] = f"https://example.com/product/{pid}#details"

            elif event_name == "add_to_cart":
                pid = last_product_id or rng.choice(products).product_id
                p = product_map[pid]
                qty_add = rng.choice([1, 1, 1, 2, 2, 3])
                cart[pid] = cart.get(pid, 0) + qty_add
                e["product_id"] = pid
                e["quantity"] = qty_add
                e["price"] = p.price
                e["page_url"] = "https://example.com/cart"
                q, v = cart_totals(cart, product_map)
                e["cart_items_qty"] = q
                e["cart_value"] = v

            elif event_name == "update_cart":
                pid = pick_cart_item(rng, cart)
                if pid is None:
                    continue
                p = product_map[pid]
                new_qty = rng.randint(1, 5)
                cart[pid] = new_qty
                e["product_id"] = pid
                e["quantity"] = new_qty
                e["price"] = p.price
                e["page_url"] = "https://example.com/cart"
                q, v = cart_totals(cart, product_map)
                e["cart_items_qty"] = q
                e["cart_value"] = v

            elif event_name == "remove_from_cart":
                pid = pick_cart_item(rng, cart)
                if pid is None:
                    continue
                p = product_map[pid]
                removed_qty = cart.get(pid, 1)
                cart.pop(pid, None)
                e["product_id"] = pid
                e["quantity"] = removed_qty
                e["price"] = p.price
                e["page_url"] = "https://example.com/cart"
                q, v = cart_totals(cart, product_map)
                e["cart_items_qty"] = q
                e["cart_value"] = v

            elif event_name == "begin_checkout":
                if not cart or in_checkout:
                    continue
                in_checkout = True
                checkout_id = uuid.uuid4().hex
                e["checkout_id"] = checkout_id
                e["page_url"] = "https://example.com/checkout"
                q, v = cart_totals(cart, product_map)
                e["cart_items_qty"] = q
                e["cart_value"] = v

            elif event_name == "checkout_progress":
                if not in_checkout or not checkout_id:
                    continue
                e["checkout_id"] = checkout_id
                e["page_url"] = f"https://example.com/checkout?step={rng.randint(1,3)}"
                q, v = cart_totals(cart, product_map)
                e["cart_items_qty"] = q
                e["cart_value"] = v

            elif event_name == "purchase":
                if not in_checkout or not checkout_id or not cart:
                    continue

                order_id = uuid.uuid4().hex
                items = []
                revenue = 0.0
                for pid, q in cart.items():
                    p = product_map[pid]
                    line_total = round(p.price * q, 2)
                    items.append(
                        {
                            "product_id": pid,
                            "name": p.name,
                            "category": p.category,
                            "price": p.price,
                            "quantity": q,
                            "line_total": line_total,
                        }
                    )
                    revenue += p.price * q

                q_total, v_total = cart_totals(cart, product_map)

                e["checkout_id"] = checkout_id
                e["order_id"] = order_id
                e["items"] = items
                e["revenue"] = round(revenue, 2)
                e["cart_items_qty"] = q_total
                e["cart_value"] = v_total
                e["page_url"] = f"https://example.com/order/{order_id}/confirmation"

                cart.clear()
                in_checkout = False
                checkout_id = None

            events.append(e)
            ts += timedelta(seconds=rng.randint(5, 140))

    return events


def write_jsonl(path: str, events: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Genera eventos dummy e-commerce (JSONL) con estructura fija.")
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