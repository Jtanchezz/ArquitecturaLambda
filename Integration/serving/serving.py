from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import redis
from dotenv import load_dotenv

# PyHive (HiveServer2)
try:
    from pyhive import hive
except Exception:
    hive = None  # se manejará con error amigable


load_dotenv()


FUNNEL_EVENTS = [
    "product_view",
    "click",
    "add_to_cart",
    "update_cart",
    "remove_from_cart",
    "begin_checkout",
    "checkout_progress",
    "purchase",
]

MARKETING_DIMENSIONS: Dict[str, Tuple[str, bool]] = {
    "channel": ("COALESCE(`marketing`.channel, 'unknown')", False),
    "source": ("COALESCE(`marketing`.source, 'unknown')", False),
    "medium": ("COALESCE(`marketing`.medium, 'unknown')", False),
    "campaign_name": ("COALESCE(`marketing`.campaign_name, 'unknown')", False),
}

CUSTOMER_DIMENSIONS: Dict[str, Tuple[str, bool]] = {
    "segment": ("COALESCE(`customer`.segment, 'unknown')", False),
    "loyalty_tier": ("COALESCE(`customer`.loyalty_tier, 'unknown')", False),
    "membership": ("CASE WHEN `customer`.is_member = true THEN 'member' ELSE 'non_member' END", False),
    "country": ("COALESCE(`geo`.country, 'unknown')", False),
}

PRODUCT_DIMENSIONS: Dict[str, str] = {
    "product_id": "COALESCE(product_id, `product`.product_id, 'unknown')",
    "category": "COALESCE(`product`.category, 'unknown')",
    "brand": "COALESCE(`product`.brand, 'unknown')",
}

OPS_DIMENSIONS: Dict[str, Tuple[str, bool]] = {
    "device_type": ("COALESCE(`device`.device_type, 'unknown')", False),
    "os": ("COALESCE(`device`.os, 'unknown')", False),
    "event_source": ("COALESCE(event_source, 'unknown')", False),
    "country": ("COALESCE(`geo`.country, 'unknown')", False),
    "city": ("COALESCE(`geo`.city, 'unknown')", False),
    "order_status": ("COALESCE(`order`.status, 'unknown')", True),
    "payment_method": ("COALESCE(`order`.payment_method, 'unknown')", True),
    "payment_provider": ("COALESCE(`order`.payment_provider, 'unknown')", True),
}


@dataclass(frozen=True)
class Settings:
    # Redis
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))
    redis_prefix: str = os.getenv("REDIS_REALTIME_PREFIX", "realtime:stats")
    watermark_key: str = os.getenv("WATERMARK_KEY", "batch:watermark")
    redis_scan_count: int = int(os.getenv("REDIS_SCAN_COUNT", "500"))

    # Hive
    hive_host: str = os.getenv("HIVE_HOST", "localhost")
    hive_port: int = int(os.getenv("HIVE_PORT", "10000"))
    hive_username: str = os.getenv("HIVE_USERNAME", "hive")
    hive_database: str = os.getenv("HIVE_DATABASE", "default")
    hive_events_table: str = os.getenv("HIVE_EVENTS_TABLE", "ecommerce_events")


SETTINGS = Settings()


def _parse_iso_z(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        ts = ts.strip()
        if ts.endswith("Z"):
            ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _purchase_amount_expr() -> str:
    return "COALESCE(revenue, `order`.total, 0.0)"


def _sql_ts(start_iso: str, end_iso: str) -> tuple[str, str]:
    return (
        start_iso.replace("T", " ").replace("Z", ""),
        end_iso.replace("T", " ").replace("Z", ""),
    )


def _time_filter(start_iso: str, end_iso: str) -> str:
    start_sql, end_sql = _sql_ts(start_iso, end_iso)
    return (
        f"event_time >= TIMESTAMP '{start_sql}' "
        f"AND event_time < TIMESTAMP '{end_sql}'"
    )


# -----------------------
# Redis (Speed layer)
# -----------------------
def redis_client(settings: Settings = SETTINGS) -> redis.Redis:
    return redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )


def get_batch_watermark(settings: Settings = SETTINGS) -> Optional[datetime]:
    r = redis_client(settings)
    raw = r.get(settings.watermark_key)
    return _parse_iso_z(raw) if raw else None


def get_realtime_stats(settings: Settings = SETTINGS, product_id: Optional[str] = None) -> pd.DataFrame:
    """
    Lee hashes tipo:
      realtime:stats:<product_id> => { event_name: count, ... }

    Devuelve:
      product_id, event_name, count

    Importante:
    este realtime sigue siendo una vista de contadores agregados por producto+evento.
    No carga revenue, marketing, customer, device ni geo, porque Redis hoy no los persiste.
    """
    r = redis_client(settings)

    if product_id:
        keys = [f"{settings.redis_prefix}:{product_id}"]
    else:
        pattern = f"{settings.redis_prefix}:*"
        keys = list(r.scan_iter(match=pattern, count=settings.redis_scan_count))

    rows: List[Dict[str, Any]] = []
    for k in keys:
        try:
            prod = k.split(":")[-1]
            h = r.hgetall(k)
            for event_name, cnt in h.items():
                try:
                    rows.append(
                        {
                            "product_id": prod,
                            "event_name": event_name,
                            "count": int(cnt),
                        }
                    )
                except Exception:
                    pass
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["product_id", "event_name", "count"])

    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype("int64")
    return df.sort_values(["count"], ascending=False)


def get_realtime_funnel(settings: Settings = SETTINGS, product_id: Optional[str] = None) -> pd.DataFrame:
    df = get_realtime_stats(settings=settings, product_id=product_id)
    if df.empty:
        return pd.DataFrame({"event_name": FUNNEL_EVENTS, "count": [0] * len(FUNNEL_EVENTS)})

    by_event = (
        df.groupby("event_name", as_index=False)["count"]
        .sum()
    )
    rows = []
    for evt in FUNNEL_EVENTS:
        cnt = by_event.loc[by_event["event_name"] == evt, "count"]
        rows.append({"event_name": evt, "count": int(cnt.iloc[0]) if not cnt.empty else 0})
    return pd.DataFrame(rows)


# -----------------------
# Hive (Batch layer)
# -----------------------
def hive_query(sql: str, settings: Settings = SETTINGS) -> pd.DataFrame:
    if hive is None:
        raise RuntimeError("PyHive no está disponible. Instala dependencias con: pip install -r requirements.txt")

    conn = hive.Connection(
        host=settings.hive_host,
        port=settings.hive_port,
        username=settings.hive_username,
        database=settings.hive_database,
    )
    try:
        return pd.read_sql(sql, conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def default_hive_sql(start_iso: str, end_iso: str, settings: Settings = SETTINGS) -> str:
    """
    SQL base por evento usando el esquema ecommerce enriquecido.
    """
    t = settings.hive_events_table
    amt = _purchase_amount_expr()
    where = _time_filter(start_iso, end_iso)
    return f"""
SELECT
  event_name,
  COUNT(*) AS events,
  COUNT(DISTINCT user_id) AS users,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN `order`.order_id END) AS orders,
  SUM(CASE WHEN event_name = 'purchase' THEN {amt} ELSE 0.0 END) AS revenue_usd
FROM {t}
WHERE {where}
GROUP BY event_name
ORDER BY events DESC
"""


def overview_hive_sql(start_iso: str, end_iso: str, settings: Settings = SETTINGS) -> str:
    t = settings.hive_events_table
    amt = _purchase_amount_expr()
    where = _time_filter(start_iso, end_iso)
    return f"""
SELECT
  COUNT(*) AS total_events,
  COUNT(DISTINCT user_id) AS users,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(DISTINCT `customer`.customer_id) AS customers,
  SUM(CASE WHEN event_name = 'product_view' THEN 1 ELSE 0 END) AS product_view,
  SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click,
  SUM(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,
  SUM(CASE WHEN event_name = 'update_cart' THEN 1 ELSE 0 END) AS update_cart,
  SUM(CASE WHEN event_name = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart,
  SUM(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout,
  SUM(CASE WHEN event_name = 'checkout_progress' THEN 1 ELSE 0 END) AS checkout_progress,
  SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchase,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN `order`.order_id END) AS distinct_orders,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_id END) AS buyers,
  SUM(CASE WHEN event_name = 'purchase' THEN {amt} ELSE 0.0 END) AS realized_revenue_usd,
  AVG(CASE WHEN event_name = 'purchase' THEN {amt} ELSE NULL END) AS avg_order_value_usd
FROM {t}
WHERE {where}
"""


def daily_series_hive_sql(start_iso: str, end_iso: str, settings: Settings = SETTINGS) -> str:
    t = settings.hive_events_table
    amt = _purchase_amount_expr()
    where = _time_filter(start_iso, end_iso)
    return f"""
SELECT
  from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd') AS time_bucket,
  COUNT(*) AS total_events,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(DISTINCT user_id) AS users,
  SUM(CASE WHEN event_name = 'product_view' THEN 1 ELSE 0 END) AS product_views,
  SUM(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,
  SUM(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout,
  SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchase_events,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN `order`.order_id END) AS orders,
  SUM(CASE WHEN event_name = 'purchase' THEN {amt} ELSE 0.0 END) AS revenue_usd
FROM {t}
WHERE {where}
GROUP BY from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd')
ORDER BY time_bucket ASC
"""


def dimension_hive_sql(
    start_iso: str,
    end_iso: str,
    expr: str,
    purchase_only: bool = False,
    settings: Settings = SETTINGS,
) -> str:
    t = settings.hive_events_table
    amt = _purchase_amount_expr()
    where = _time_filter(start_iso, end_iso)
    extra = " AND event_name = 'purchase'" if purchase_only else ""
    return f"""
SELECT
  {expr} AS dimension,
  COUNT(*) AS events,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(DISTINCT user_id) AS users,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN `order`.order_id END) AS orders,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_id END) AS buyers,
  SUM(CASE WHEN event_name = 'purchase' THEN {amt} ELSE 0.0 END) AS revenue_usd
FROM {t}
WHERE {where}{extra}
GROUP BY {expr}
ORDER BY revenue_usd DESC, orders DESC, events DESC
"""


def product_hive_sql(start_iso: str, end_iso: str, expr: str, settings: Settings = SETTINGS) -> str:
    t = settings.hive_events_table
    amt = _purchase_amount_expr()
    where = _time_filter(start_iso, end_iso)
    return f"""
SELECT
  {expr} AS dimension,
  SUM(CASE WHEN event_name = 'product_view' THEN 1 ELSE 0 END) AS product_views,
  SUM(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,
  SUM(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout,
  SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchase_events,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN `order`.order_id END) AS orders,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_id END) AS buyers,
  SUM(CASE WHEN event_name = 'purchase' THEN {amt} ELSE 0.0 END) AS revenue_usd
FROM {t}
WHERE {where}
  AND {expr} <> 'unknown'
GROUP BY {expr}
ORDER BY revenue_usd DESC, orders DESC, product_views DESC
"""


def heatmap_event_day_hive_sql(start_iso: str, end_iso: str, settings: Settings = SETTINGS) -> str:
    t = settings.hive_events_table
    where = _time_filter(start_iso, end_iso)
    return f"""
SELECT
  from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd') AS time_bucket,
  event_name,
  COUNT(*) AS cnt
FROM {t}
WHERE {where}
GROUP BY from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd'), event_name
"""


def hour_distribution_hive_sql(start_iso: str, end_iso: str, settings: Settings = SETTINGS) -> str:
    t = settings.hive_events_table
    where = _time_filter(start_iso, end_iso)
    return f"""
SELECT
  hour(event_time) AS hour_of_day,
  COUNT(*) AS cnt,
  SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchase_events
FROM {t}
WHERE {where}
GROUP BY hour(event_time)
ORDER BY hour_of_day ASC
"""


# -----------------------
# Lambda view (combinada)
# -----------------------
def lambda_view_note() -> str:
    return (
        "Batch usa el esquema ecommerce completo desde Hive. "
        "Redis sigue exponiendo un snapshot agregado por product_id + event_name, "
        "sin ventana temporal y sin atributos enriquecidos (marketing, customer, device, geo, revenue). "
        "Por eso esta vista compara composición de eventos entre histórico Batch y snapshot Speed."
    )
