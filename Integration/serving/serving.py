from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import pandas as pd
import redis
from dotenv import load_dotenv

# PyHive (HiveServer2)
try:
    from pyhive import hive
except Exception:
    hive = None  # se manejará con error amigable


load_dotenv()


@dataclass(frozen=True)
class Settings:
    # Redis
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))
    redis_prefix: str = os.getenv("REDIS_REALTIME_PREFIX", "realtime:stats")
    watermark_key: str = os.getenv("WATERMARK_KEY", "batch:watermark")

    # Hive
    hive_host: str = os.getenv("HIVE_HOST", "localhost")
    hive_port: int = int(os.getenv("HIVE_PORT", "10000"))
    hive_username: str = os.getenv("HIVE_USERNAME", "hive")
    hive_database: str = os.getenv("HIVE_DATABASE", "default")
    hive_events_table: str = os.getenv("HIVE_EVENTS_TABLE", "ecommerce_events")


SETTINGS = Settings()


def _parse_iso_z(ts: str) -> Optional[datetime]:
    """
    Acepta '2026-02-28T10:00:00Z' o ISO sin Z.
    """
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
    """
    Lee watermark del batch desde Redis key: batch:watermark
    """
    r = redis_client(settings)
    raw = r.get(settings.watermark_key)
    return _parse_iso_z(raw) if raw else None


def get_realtime_stats(settings: Settings = SETTINGS, product_id: Optional[str] = None) -> pd.DataFrame:
    """
    Lee hashes tipo:
      realtime:stats:<product_id>  => { event_name: count, ... }

    Devuelve DataFrame: product_id, event_name, count
    """
    r = redis_client(settings)

    if product_id:
        keys = [f"{settings.redis_prefix}:{product_id}"]
    else:
        pattern = f"{settings.redis_prefix}:*"
        keys = list(r.scan_iter(match=pattern, count=500))

    rows: List[Dict[str, Any]] = []
    for k in keys:
        try:
            prod = k.split(":")[-1]
            h = r.hgetall(k)
            for event_name, cnt in h.items():
                try:
                    rows.append({"product_id": prod, "event_name": event_name, "count": int(cnt)})
                except Exception:
                    pass
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["product_id", "event_name", "count"])

    df["count"] = df["count"].astype("int64")
    return df.sort_values(["count"], ascending=False)


# -----------------------
# Hive (Batch layer)
# -----------------------
def hive_query(sql: str, settings: Settings = SETTINGS) -> pd.DataFrame:
    """
    Ejecuta SQL en HiveServer2.
    Requiere que tu docker-compose batch exponga 10000:10000 (ya lo hace).
    """
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
    SQL base (ajústalo cuando crees tu tabla en Hive).
    Espera columnas:
      - event_time (timestamp)
      - event_name (string)
    """
    t = settings.hive_events_table
    return f"""
SELECT
  event_name,
  COUNT(*) AS cnt
FROM {t}
WHERE event_time >= TIMESTAMP '{start_iso.replace("T"," ").replace("Z","")}'
  AND event_time <  TIMESTAMP '{end_iso.replace("T"," ").replace("Z","")}'
GROUP BY event_name
ORDER BY cnt DESC
"""


# -----------------------
# Lambda view (combinada)
# -----------------------
def lambda_view_note() -> str:
    return (
        "Nota: Redis actualmente guarda contadores acumulados por product_id+event_name "
        "sin ventana de tiempo. Por eso, la 'Lambda View' aquí se muestra como: "
        "Histórico (Hive) + Snapshot realtime (Redis). "
        "Luego lo hacemos exacto agregando llaves por ventana (minuto/día) o guardando ventanas."
    )