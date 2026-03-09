#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera eventos dummy e-commerce en JSONL (1 JSON por línea) con esta estructura fija.

Uso batch:
  python generate_events.py --n 200 --stdout

Uso stream por 2 minutos:
  python generate_events.py --stream --stdout

Uso recomendado para Kafka:
  python generate_events.py --stream --stdout | kafka-console-producer ...
"""

import argparse
import json
import random
import time
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


@dataclass
class SessionState:
    user_id: str
    session_id: str
    device: str
    currency: str
    referrer: str
    cart_id: str
    cart: Dict[str, int]
    in_checkout: bool
    checkout_id: Optional[str]
    last_product_id: Optional[str]
    remaining_steps: int


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


def new_session(users: List[str], rng: random.Random) -> SessionState:
    return SessionState(
        user_id=rng.choice(users),
        session_id=uuid.uuid4().hex,
        device=rng.choice(DEVICES),
        currency=rng.choice(CURRENCIES),
        referrer=rng.choice(REFERRERS),
        cart_id=uuid.uuid4().hex,
        cart={},
        in_checkout=False,
        checkout_id=None,
        last_product_id=None,
        remaining_steps=rng.randint(8, 35),
    )


def advance_session(
    state: SessionState,
    *,
    rng: random.Random,
    products: List[Product],
    product_map: Dict[str, Product],
    ts: datetime,
) -> Tuple[Optional[dict], bool]:
    if state.remaining_steps <= 0:
        return None, True

    choices: List[Tuple[str, float]] = [
        ("product_view", 0.36),
        ("click", 0.20 if state.last_product_id else 0.10),
        ("add_to_cart", 0.16 if state.last_product_id else 0.06),
    ]
    if state.cart:
        choices += [
            ("update_cart", 0.06),
            ("remove_from_cart", 0.06),
            ("begin_checkout", 0.06 if not state.in_checkout else 0.0),
        ]
    if state.in_checkout:
        choices += [
            ("checkout_progress", 0.07),
            ("purchase", 0.05),
        ]
    choices.append(("end_session", 0.02))

    event_name = weighted_choice(rng, choices)
    state.remaining_steps -= 1

    if event_name == "end_session":
        return None, True

    e = base_event(
        event_name=event_name,
        ts=ts,
        user_id=state.user_id,
        session_id=state.session_id,
        device=state.device,
        currency=state.currency,
        cart_id=state.cart_id,
        referrer=state.referrer,
    )

    if event_name == "product_view":
        p = rng.choice(products)
        state.last_product_id = p.product_id
        e["product_id"] = p.product_id
        e["price"] = p.price
        e["page_url"] = f"https://example.com/product/{p.product_id}"

    elif event_name == "click":
        pid = state.last_product_id or rng.choice(products).product_id
        p = product_map[pid]
        state.last_product_id = pid
        e["product_id"] = pid
        e["price"] = p.price
        e["page_url"] = f"https://example.com/product/{pid}#details"

    elif event_name == "add_to_cart":
        pid = state.last_product_id or rng.choice(products).product_id
        p = product_map[pid]
        qty_add = rng.choice([1, 1, 1, 2, 2, 3])
        state.cart[pid] = state.cart.get(pid, 0) + qty_add
        e["product_id"] = pid
        e["quantity"] = qty_add
        e["price"] = p.price
        e["page_url"] = "https://example.com/cart"
        q, v = cart_totals(state.cart, product_map)
        e["cart_items_qty"] = q
        e["cart_value"] = v

    elif event_name == "update_cart":
        pid = pick_cart_item(rng, state.cart)
        if pid is None:
            return None, False
        p = product_map[pid]
        new_qty = rng.randint(1, 5)
        state.cart[pid] = new_qty
        e["product_id"] = pid
        e["quantity"] = new_qty
        e["price"] = p.price
        e["page_url"] = "https://example.com/cart"
        q, v = cart_totals(state.cart, product_map)
        e["cart_items_qty"] = q
        e["cart_value"] = v

    elif event_name == "remove_from_cart":
        pid = pick_cart_item(rng, state.cart)
        if pid is None:
            return None, False
        p = product_map[pid]
        removed_qty = state.cart.get(pid, 1)
        state.cart.pop(pid, None)
        e["product_id"] = pid
        e["quantity"] = removed_qty
        e["price"] = p.price
        e["page_url"] = "https://example.com/cart"
        q, v = cart_totals(state.cart, product_map)
        e["cart_items_qty"] = q
        e["cart_value"] = v

    elif event_name == "begin_checkout":
        if not state.cart or state.in_checkout:
            return None, False
        state.in_checkout = True
        state.checkout_id = uuid.uuid4().hex
        e["checkout_id"] = state.checkout_id
        e["page_url"] = "https://example.com/checkout"
        q, v = cart_totals(state.cart, product_map)
        e["cart_items_qty"] = q
        e["cart_value"] = v

    elif event_name == "checkout_progress":
        if not state.in_checkout or not state.checkout_id:
            return None, False
        e["checkout_id"] = state.checkout_id
        e["page_url"] = f"https://example.com/checkout?step={rng.randint(1,3)}"
        q, v = cart_totals(state.cart, product_map)
        e["cart_items_qty"] = q
        e["cart_value"] = v

    elif event_name == "purchase":
        if not state.in_checkout or not state.checkout_id or not state.cart:
            return None, False

        order_id = uuid.uuid4().hex
        items = []
        revenue = 0.0
        for pid, q in state.cart.items():
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

        q_total, v_total = cart_totals(state.cart, product_map)

        e["checkout_id"] = state.checkout_id
        e["order_id"] = order_id
        e["items"] = items
        e["revenue"] = round(revenue, 2)
        e["cart_items_qty"] = q_total
        e["cart_value"] = v_total
        e["page_url"] = f"https://example.com/order/{order_id}/confirmation"

        state.cart.clear()
        state.in_checkout = False
        state.checkout_id = None

    done = state.remaining_steps <= 0
    return e, done


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
        state = new_session(users, rng)
        ts = random_start_time()

        while len(events) < n_events:
            e, done = advance_session(state, rng=rng, products=products, product_map=product_map, ts=ts)
            ts += timedelta(seconds=rng.randint(5, 140))
            if e is not None:
                events.append(e)
            if done:
                break

    return events[:n_events]


def write_jsonl(path: str, events: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def stream_events(
    *,
    duration_seconds: int,
    events_per_second: float,
    seed: int,
    n_users: int,
    n_products: int,
    out_path: Optional[str],
    to_stdout: bool,
) -> None:
    if events_per_second <= 0:
        raise ValueError("--eps debe ser > 0")

    rng = random.Random(seed)
    users = make_users(n_users)
    products = make_products(n_products, rng)
    product_map = {p.product_id: p for p in products}

    end_time = time.monotonic() + duration_seconds
    interval = 1.0 / events_per_second
    next_emit = time.monotonic()

    emitted = 0
    state: Optional[SessionState] = None

    file_handle = None
    try:
        if not to_stdout:
            file_handle = open(out_path or "events.jsonl", "a", encoding="utf-8")

        while time.monotonic() < end_time:
            if state is None:
                state = new_session(users, rng)

            event = None
            done = False
            while event is None:
                event, done = advance_session(
                    state,
                    rng=rng,
                    products=products,
                    product_map=product_map,
                    ts=datetime.now(timezone.utc),
                )
                if done:
                    state = None
                    if event is None:
                        break

            if event is not None:
                line = json.dumps(event, ensure_ascii=False)
                if to_stdout:
                    print(line, flush=True)
                else:
                    file_handle.write(line + "\n")
                    file_handle.flush()
                emitted += 1

            next_emit += interval
            sleep_for = next_emit - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

        if not to_stdout:
            print(f"OK: {emitted} eventos emitidos en {duration_seconds}s -> {out_path or 'events.jsonl'}")
    finally:
        if file_handle is not None:
            file_handle.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Genera eventos dummy e-commerce (JSONL) con estructura fija.")
    ap.add_argument("--n", type=int, default=1000, help="Cantidad total de eventos en modo batch (default: 1000)")
    ap.add_argument("--seed", type=int, default=42, help="Semilla RNG (default: 42)")
    ap.add_argument("--users", type=int, default=200, help="Cantidad de usuarios (default: 200)")
    ap.add_argument("--products", type=int, default=120, help="Cantidad de productos (default: 120)")
    ap.add_argument("--days-back", type=int, default=14, help="Ventana temporal hacia atrás en batch (default: 14 días)")
    ap.add_argument("--out", default="events.jsonl", help="Archivo de salida JSONL (default: events.jsonl)")
    ap.add_argument("--stdout", action="store_true", help="Imprimir eventos a stdout (JSON por línea)")
    ap.add_argument("--stream", action="store_true", help="Emitir eventos continuamente")
    ap.add_argument("--duration-seconds", type=int, default=120, help="Duración del stream en segundos (default: 120)")
    ap.add_argument("--eps", type=float, default=5.0, help="Eventos por segundo en modo stream (default: 5.0)")
    args = ap.parse_args()

    if args.stream:
        stream_events(
            duration_seconds=args.duration_seconds,
            events_per_second=args.eps,
            seed=args.seed,
            n_users=args.users,
            n_products=args.products,
            out_path=args.out,
            to_stdout=args.stdout,
        )
        return

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
