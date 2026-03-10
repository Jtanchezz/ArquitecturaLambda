from datetime import datetime, timedelta, timezone
import streamlit as st
import pandas as pd

# Altair (opcional)
try:
    import altair as alt
except Exception:
    alt = None


# -----------------------------
# Config (DEBE ir primero)
# -----------------------------
st.set_page_config(
    page_title="Lambda Architecture - Business View",
    layout="wide",
)

# -----------------------------
# Import robusto de serving
# -----------------------------
try:
    import serving

    if not hasattr(serving, "SETTINGS") and hasattr(serving, "Settings"):
        serving.SETTINGS = serving.Settings()

    if not hasattr(serving, "SETTINGS"):
        raise RuntimeError("serving.py no define SETTINGS (ni se pudo reconstruir).")

except Exception as e:
    st.title("Serving Layer (Lambda Architecture)")
    st.error(f"No pude cargar `serving.py`: {e}")
    st.stop()

SETTINGS = serving.SETTINGS


# -----------------------------
# Constantes de negocio
# -----------------------------
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

MARKETING_DIMENSIONS = {
    "Canal": ("COALESCE(`marketing`.channel, 'unknown')", False),
    "Fuente": ("COALESCE(`marketing`.source, 'unknown')", False),
    "Medio": ("COALESCE(`marketing`.medium, 'unknown')", False),
    "Campaña": ("COALESCE(`marketing`.campaign_name, 'unknown')", False),
}

CUSTOMER_DIMENSIONS = {
    "Segmento": ("COALESCE(`customer`.segment, 'unknown')", False),
    "Loyalty tier": ("COALESCE(`customer`.loyalty_tier, 'unknown')", False),
    "Membership": ("CASE WHEN `customer`.is_member = true THEN 'member' ELSE 'non_member' END", False),
    "País cliente": ("COALESCE(`geo`.country, 'unknown')", False),
}

PRODUCT_DIMENSIONS = {
    "Producto": "COALESCE(product_id, `product`.product_id, 'unknown')",
    "Categoría": "COALESCE(`product`.category, 'unknown')",
    "Marca": "COALESCE(`product`.brand, 'unknown')",
}

OPS_DIMENSIONS = {
    "Device type": ("COALESCE(`device`.device_type, 'unknown')", False),
    "OS": ("COALESCE(`device`.os, 'unknown')", False),
    "Event source": ("COALESCE(event_source, 'unknown')", False),
    "Country": ("COALESCE(`geo`.country, 'unknown')", False),
    "City": ("COALESCE(`geo`.city, 'unknown')", False),
    "Order status": ("COALESCE(`order`.status, 'unknown')", True),
    "Payment method": ("COALESCE(`order`.payment_method, 'unknown')", True),
    "Payment provider": ("COALESCE(`order`.payment_provider, 'unknown')", True),
}


# -----------------------------
# Helpers
# -----------------------------
def _fmt_int(n) -> str:
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return str(n)


def _fmt_pct(x: float) -> str:
    try:
        return f"{100.0 * float(x):.1f}%"
    except Exception:
        return str(x)


def _fmt_money(x: float) -> str:
    try:
        return f"US$ {float(x):,.2f}"
    except Exception:
        return str(x)


def _safe_int_series(s: pd.Series) -> pd.Series:
    try:
        return s.fillna(0).astype("int64")
    except Exception:
        return pd.to_numeric(s, errors="coerce").fillna(0).astype("int64")


def _safe_float_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0).astype(float)


def _ratio(num, den) -> float:
    try:
        num = float(num or 0)
        den = float(den or 0)
        return num / den if den else 0.0
    except Exception:
        return 0.0


def _rerun():
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()


def add_share(df: pd.DataFrame, value_col: str, share_col: str = "share") -> pd.DataFrame:
    if df is None or df.empty or value_col not in df.columns:
        out = df.copy() if df is not None else pd.DataFrame()
        if share_col not in out.columns:
            out[share_col] = 0.0
        return out

    out = df.copy()
    total = pd.to_numeric(out[value_col], errors="coerce").fillna(0).sum()
    if total <= 0:
        out[share_col] = 0.0
    else:
        out[share_col] = pd.to_numeric(out[value_col], errors="coerce").fillna(0) / total
    return out


def top_share(df: pd.DataFrame, value_col: str) -> float:
    if df is None or df.empty or value_col not in df.columns:
        return 0.0
    s = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
    total = s.sum()
    if total <= 0:
        return 0.0
    return float(s.max() / total)


def top_n_share(df: pd.DataFrame, value_col: str, n: int) -> float:
    if df is None or df.empty or value_col not in df.columns:
        return 0.0
    s = pd.to_numeric(df[value_col], errors="coerce").fillna(0).sort_values(ascending=False)
    total = s.sum()
    if total <= 0:
        return 0.0
    return float(s.head(n).sum() / total)


def _value_from_df(df: pd.DataFrame, key_col: str, key_val: str, value_col: str) -> float:
    if df is None or df.empty:
        return 0.0
    if key_col not in df.columns or value_col not in df.columns:
        return 0.0
    tmp = df[df[key_col] == key_val]
    if tmp.empty:
        return 0.0
    return float(pd.to_numeric(tmp[value_col], errors="coerce").fillna(0).sum())


def _purchase_amount_expr() -> str:
    return "COALESCE(revenue, `order`.total, 0.0)"


def _time_filter(start_sql: str, end_sql: str) -> str:
    return (
        f"event_time >= TIMESTAMP '{start_sql}' "
        f"AND event_time < TIMESTAMP '{end_sql}'"
    )


# -----------------------------
# Realtime helpers
# -----------------------------
def realtime_business_tables(df_rt: pd.DataFrame):
    if df_rt is None or df_rt.empty:
        empty_event = pd.DataFrame(columns=["event_name", "count", "active_products", "share", "avg_per_active_product"])
        empty_product = pd.DataFrame(columns=["product_id", "count", "event_types", "share"])
        empty_mix = pd.DataFrame(columns=["product_id", "event_name", "count"])
        return empty_event, empty_product, empty_mix

    df = df_rt.copy()
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)

    by_event = (
        df.groupby("event_name", as_index=False)
        .agg(
            count=("count", "sum"),
            active_products=("product_id", "nunique"),
        )
        .sort_values("count", ascending=False)
    )
    by_event = add_share(by_event, "count", "share")
    by_event["avg_per_active_product"] = (
        by_event["count"] / by_event["active_products"].replace(0, pd.NA)
    ).fillna(0)

    by_product = (
        df.groupby("product_id", as_index=False)
        .agg(
            count=("count", "sum"),
            event_types=("event_name", "nunique"),
        )
        .sort_values("count", ascending=False)
    )
    by_product = add_share(by_product, "count", "share")

    mix = (
        df.groupby(["product_id", "event_name"], as_index=False)["count"]
        .sum()
    )

    return by_event, by_product, mix


def realtime_funnel_snapshot(df_rt: pd.DataFrame) -> pd.DataFrame:
    if df_rt is None or df_rt.empty:
        out = pd.DataFrame(
            {
                "event_name": FUNNEL_EVENTS,
                "count": [0] * len(FUNNEL_EVENTS),
            }
        )
        return add_share(out, "count", "share")

    by_event, _, _ = realtime_business_tables(df_rt)
    rows = []
    for evt in FUNNEL_EVENTS:
        rows.append(
            {
                "event_name": evt,
                "count": int(_value_from_df(by_event, "event_name", evt, "count")),
            }
        )
    out = pd.DataFrame(rows)
    out = add_share(out, "count", "share")
    return out


def realtime_purchase_products(df_rt: pd.DataFrame) -> pd.DataFrame:
    if df_rt is None or df_rt.empty:
        return pd.DataFrame(columns=["product_id", "purchase_count", "share"])

    tmp = df_rt[df_rt["event_name"] == "purchase"].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["product_id", "purchase_count", "share"])

    out = (
        tmp.groupby("product_id", as_index=False)["count"]
        .sum()
        .rename(columns={"count": "purchase_count"})
        .sort_values("purchase_count", ascending=False)
    )
    out = add_share(out, "purchase_count", "share")
    return out


def realtime_kpis(df_rt: pd.DataFrame) -> dict:
    if df_rt is None or df_rt.empty:
        return {
            "total_interactions": 0,
            "active_products": 0,
            "event_types": 0,
            "avg_per_product": 0,
            "top_product_share": 0.0,
            "top5_product_share": 0.0,
            "top_event_share": 0.0,
            "top3_event_share": 0.0,
            "purchase_share": 0.0,
            "view_to_cart": 0.0,
            "cart_to_checkout": 0.0,
            "checkout_to_purchase": 0.0,
        }

    by_event, by_product, _ = realtime_business_tables(df_rt)
    total_interactions = int(pd.to_numeric(df_rt["count"], errors="coerce").fillna(0).sum())
    active_products = int(df_rt["product_id"].nunique())
    event_types = int(df_rt["event_name"].nunique())
    avg_per_product = int(total_interactions / active_products) if active_products else 0

    views = _value_from_df(by_event, "event_name", "product_view", "count")
    carts = _value_from_df(by_event, "event_name", "add_to_cart", "count")
    checkouts = _value_from_df(by_event, "event_name", "begin_checkout", "count")
    purchases = _value_from_df(by_event, "event_name", "purchase", "count")

    return {
        "total_interactions": total_interactions,
        "active_products": active_products,
        "event_types": event_types,
        "avg_per_product": avg_per_product,
        "top_product_share": top_share(by_product, "count"),
        "top5_product_share": top_n_share(by_product, "count", 5),
        "top_event_share": top_share(by_event, "count"),
        "top3_event_share": top_n_share(by_event, "count", 3),
        "purchase_share": _ratio(purchases, total_interactions),
        "view_to_cart": _ratio(carts, views),
        "cart_to_checkout": _ratio(checkouts, carts),
        "checkout_to_purchase": _ratio(purchases, checkouts),
    }


# -----------------------------
# Batch / Hive helpers
# -----------------------------
def batch_kpis_from_df(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "rows": 0,
            "total": 0,
            "series": 0,
            "top_share": 0.0,
        }

    rows = int(len(df))
    numeric_cols = [c for c in df.columns if c.lower() in ("cnt", "count", "counts", "events", "orders", "revenue_usd")]
    total = int(pd.to_numeric(df[numeric_cols[0]], errors="coerce").fillna(0).sum()) if numeric_cols else 0

    series = 0
    for c in ("event_name", "dimension", "product_id", "time_bucket", "hour_of_day"):
        if c in df.columns:
            series = int(df[c].nunique())
            break

    top_share_value = 0.0
    if numeric_cols:
        top_share_value = top_share(df, numeric_cols[0])

    return {"rows": rows, "total": total, "series": series, "top_share": top_share_value}


def batch_event_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "event_name" not in df.columns:
        return pd.DataFrame(columns=["event_name", "batch_cnt", "batch_share"])

    if "events" in df.columns:
        value_col = "events"
    elif "cnt" in df.columns:
        value_col = "cnt"
    elif "count" in df.columns:
        value_col = "count"
    else:
        return pd.DataFrame(columns=["event_name", "batch_cnt", "batch_share"])

    out = (
        df.groupby("event_name", as_index=False)[value_col]
        .sum()
        .rename(columns={value_col: "batch_cnt"})
    )
    out = add_share(out, "batch_cnt", "batch_share")
    return out


# -----------------------------
# SQL builders
# -----------------------------
def sql_overview(t: str, start_sql: str, end_sql: str) -> str:
    amt = _purchase_amount_expr()
    where = _time_filter(start_sql, end_sql)
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


def sql_event_mix(t: str, start_sql: str, end_sql: str) -> str:
    amt = _purchase_amount_expr()
    where = _time_filter(start_sql, end_sql)
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


def sql_daily_series(t: str, start_sql: str, end_sql: str) -> str:
    amt = _purchase_amount_expr()
    where = _time_filter(start_sql, end_sql)
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


def sql_dimension_generic(t: str, start_sql: str, end_sql: str, expr: str, purchase_only: bool = False) -> str:
    amt = _purchase_amount_expr()
    where = _time_filter(start_sql, end_sql)
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


def sql_product_dimension(t: str, start_sql: str, end_sql: str, expr: str) -> str:
    amt = _purchase_amount_expr()
    where = _time_filter(start_sql, end_sql)
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


def sql_heatmap_event_day(t: str, start_sql: str, end_sql: str) -> str:
    where = _time_filter(start_sql, end_sql)
    return f"""
SELECT
  from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd') AS time_bucket,
  event_name,
  COUNT(*) AS cnt
FROM {t}
WHERE {where}
GROUP BY from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd'), event_name
"""


def sql_hour_distribution(t: str, start_sql: str, end_sql: str) -> str:
    where = _time_filter(start_sql, end_sql)
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


# -----------------------------
# Charts
# -----------------------------
def bar_chart(df: pd.DataFrame, x: str, y: str, title: str, top_n: int = 20, height: int = 360):
    if df is None or df.empty:
        st.info("No hay datos para graficar.")
        return

    dfp = df.copy().sort_values(y, ascending=False).head(top_n)

    st.subheader(title)
    if alt is None:
        tmp = dfp[[x, y]].set_index(x)
        st.bar_chart(tmp, height=height)
        return

    x_type = "Q" if pd.api.types.is_numeric_dtype(dfp[x]) else "N"

    chart = (
        alt.Chart(dfp)
        .mark_bar()
        .encode(
            x=alt.X(f"{y}:Q", title=y),
            y=alt.Y(f"{x}:{x_type}", sort="-x", title=""),
            tooltip=[alt.Tooltip(f"{x}:{x_type}"), alt.Tooltip(f"{y}:Q")],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def ordered_bar_chart(df: pd.DataFrame, x: str, y: str, title: str, order: list[str], height: int = 320):
    if df is None or df.empty:
        st.info("No hay datos para graficar.")
        return

    st.subheader(title)
    if alt is None:
        st.dataframe(df[[x, y]], use_container_width=True, height=height)
        return

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{x}:N", sort=order, title=""),
            y=alt.Y(f"{y}:Q", title=y),
            tooltip=[alt.Tooltip(f"{x}:N"), alt.Tooltip(f"{y}:Q")],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def line_chart(df: pd.DataFrame, x: str, y: str, title: str, height: int = 320):
    if df is None or df.empty:
        st.info("No hay datos para graficar.")
        return

    dfp = df.copy()
    st.subheader(title)

    if alt is None:
        tmp = dfp[[x, y]].set_index(x)
        st.line_chart(tmp, height=height)
        return

    chart = (
        alt.Chart(dfp)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:T", title="Tiempo"),
            y=alt.Y(f"{y}:Q", title=y),
            tooltip=[alt.Tooltip(f"{x}:T"), alt.Tooltip(f"{y}:Q")],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def heatmap_chart(df: pd.DataFrame, x: str, y: str, value: str, title: str, height: int = 420):
    if df is None or df.empty:
        st.info("No hay datos para graficar.")
        return

    st.subheader(title)

    if alt is None:
        pivot = df.pivot_table(index=y, columns=x, values=value, aggfunc="sum", fill_value=0)
        st.dataframe(pivot, use_container_width=True, height=height)
        return

    chart = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=alt.X(f"{x}:N", title=""),
            y=alt.Y(f"{y}:N", title=""),
            color=alt.Color(f"{value}:Q", title=value),
            tooltip=[alt.Tooltip(f"{x}:N"), alt.Tooltip(f"{y}:N"), alt.Tooltip(f"{value}:Q")],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def pareto_chart(series_df: pd.DataFrame, category: str, value: str, title: str, top_n: int = 30, height: int = 360):
    if series_df is None or series_df.empty:
        st.info("No hay datos para graficar.")
        return

    dfp = series_df[[category, value]].copy()
    dfp[value] = pd.to_numeric(dfp[value], errors="coerce").fillna(0)
    dfp = dfp.sort_values(value, ascending=False).head(top_n)
    total = dfp[value].sum()
    if total <= 0:
        st.info("No hay valores positivos para Pareto.")
        return

    dfp["cumulative"] = dfp[value].cumsum()
    dfp["cumulative_pct"] = (dfp["cumulative"] / total) * 100.0

    st.subheader(title)

    if alt is None:
        st.dataframe(dfp, use_container_width=True, height=height)
        return

    bars = (
        alt.Chart(dfp)
        .mark_bar()
        .encode(
            x=alt.X(f"{category}:N", sort=None, title=""),
            y=alt.Y(f"{value}:Q", title=value),
            tooltip=[
                alt.Tooltip(f"{category}:N"),
                alt.Tooltip(f"{value}:Q"),
                alt.Tooltip("cumulative_pct:Q", title="Acumulado %"),
            ],
        )
    )

    line = (
        alt.Chart(dfp)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{category}:N", sort=None),
            y=alt.Y("cumulative_pct:Q", title="Acumulado (%)"),
            tooltip=[alt.Tooltip("cumulative_pct:Q", title="Acumulado %")],
        )
    )

    chart = alt.layer(bars, line).resolve_scale(y="independent").properties(height=height)
    st.altair_chart(chart, use_container_width=True)


def grouped_compare_chart(df: pd.DataFrame, category: str, a: str, b: str, title: str, top_n: int = 20, height: int = 420):
    if df is None or df.empty:
        st.info("No hay datos para comparar.")
        return

    dfp = df.copy()
    dfp["__max"] = dfp[[a, b]].max(axis=1)
    dfp = dfp.sort_values("__max", ascending=False).head(top_n)

    st.subheader(title)

    if alt is None:
        st.dataframe(dfp[[category, a, b]], use_container_width=True, height=height)
        return

    melted = dfp[[category, a, b]].melt(id_vars=[category], var_name="layer", value_name="value")
    chart = (
        alt.Chart(melted)
        .mark_bar()
        .encode(
            x=alt.X("value:Q", title="Eventos"),
            y=alt.Y(f"{category}:N", sort="-x", title=""),
            color="layer:N",
            tooltip=[alt.Tooltip(f"{category}:N"), alt.Tooltip("layer:N"), alt.Tooltip("value:Q")],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


# -----------------------------
# Cache
# -----------------------------
@st.cache_data(ttl=60, show_spinner=False)
def cached_hive_query(sql: str) -> pd.DataFrame:
    return serving.hive_query(sql)


@st.cache_data(ttl=5, show_spinner=False)
def cached_realtime_stats(product_id: str | None) -> pd.DataFrame:
    return serving.get_realtime_stats(product_id=product_id)


# -----------------------------
# Header
# -----------------------------
st.title("Serving Layer - Lambda Architecture")
st.caption(
    "Batch (Hive) aprovecha el esquema ecommerce completo. "
    "Realtime (Redis) mantiene compatibilidad con el snapshot actual por product_id + event_name."
)

# -----------------------------
# Sidebar
# -----------------------------
now = datetime.now(timezone.utc).replace(microsecond=0)
default_start = (now - timedelta(hours=24)).replace(microsecond=0)

with st.sidebar:
    st.header("Filtros")

    product_filter = st.text_input("product_id (opcional)", value="").strip() or None
    top_n = st.slider("Top N (graficas)", min_value=5, max_value=50, value=15, step=1)

    st.divider()
    st.subheader("Rango historico (UTC)")
    cA, cB = st.columns(2)
    start = cA.date_input("Inicio", value=default_start.date())
    end = cB.date_input("Fin", value=now.date())

    start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)

    if end_dt <= start_dt:
        st.warning("Rango invalido: Fin debe ser >= Inicio.")

    start_iso = start_dt.isoformat().replace("+00:00", "Z")
    end_iso = end_dt.isoformat().replace("+00:00", "Z")
    start_sql = start_iso.replace("T", " ").replace("Z", "")
    end_sql = end_iso.replace("T", " ").replace("Z", "")
    hive_table = SETTINGS.hive_events_table

    st.divider()

    if st.button("Actualizar", use_container_width=True):
        cached_hive_query.clear()
        cached_realtime_stats.clear()
        _rerun()

    st.caption(
        "Revenue se calcula desde `revenue` y, si viene nulo, desde `order.total`. "
        "En Redis el realtime sigue limitado a contadores por evento."
    )

tab_rt, tab_hist, tab_lambda = st.tabs(
    ["Realtime (Redis)", "Historico (Hive)", "Lambda View (Batch + Speed)"]
)

# -----------------------------
# TAB: Realtime (Redis)
# -----------------------------
with tab_rt:
    topbar_l, topbar_r = st.columns([1, 3])
    with topbar_l:
        if st.button("Actualizar realtime", use_container_width=True):
            cached_realtime_stats.clear()
            _rerun()
    with topbar_r:
        st.caption("Snapshot acumulado por producto y evento, compatible con el esquema Redis actual.")

    try:
        df_rt = cached_realtime_stats(product_filter)
    except Exception as e:
        df_rt = pd.DataFrame(columns=["product_id", "event_name", "count"])
        st.warning(f"Redis no disponible o sin datos: {e}")

    by_event, by_product, mix_matrix = realtime_business_tables(df_rt)
    funnel_rt = realtime_funnel_snapshot(df_rt)
    purchase_rt = realtime_purchase_products(df_rt)

    rt_tabs = st.tabs(
        [
            "Resumen snapshot",
            "Funnel snapshot",
            "Mix de eventos",
            "Actividad por producto",
            "Drilldown producto",
        ]
    )

    with rt_tabs[0]:
        k = realtime_kpis(df_rt)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Eventos totales", _fmt_int(k["total_interactions"]))
        c2.metric("Productos activos", _fmt_int(k["active_products"]))
        c3.metric("Tipos de evento", _fmt_int(k["event_types"]))
        c4.metric("Intensidad por producto", _fmt_int(k["avg_per_product"]))

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Peso top producto", _fmt_pct(k["top_product_share"]))
        c6.metric("Peso top 5 productos", _fmt_pct(k["top5_product_share"]))
        c7.metric("Peso top evento", _fmt_pct(k["top_event_share"]))
        c8.metric("Peso purchase", _fmt_pct(k["purchase_share"]))

        st.divider()
        st.subheader("Detalle snapshot")
        st.dataframe(df_rt, use_container_width=True, height=440)

        csv = df_rt.to_csv(index=False).encode("utf-8")
        st.download_button("Descargar CSV (Realtime)", csv, "realtime_snapshot.csv", "text/csv")

    with rt_tabs[1]:
        views = int(_value_from_df(funnel_rt, "event_name", "product_view", "count"))
        carts = int(_value_from_df(funnel_rt, "event_name", "add_to_cart", "count"))
        checkouts = int(_value_from_df(funnel_rt, "event_name", "begin_checkout", "count"))
        purchases = int(_value_from_df(funnel_rt, "event_name", "purchase", "count"))

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("View → Cart", _fmt_pct(_ratio(carts, views)))
        c2.metric("Cart → Checkout", _fmt_pct(_ratio(checkouts, carts)))
        c3.metric("Checkout → Purchase", _fmt_pct(_ratio(purchases, checkouts)))
        c4.metric("Purchase / total", _fmt_pct(_ratio(purchases, funnel_rt["count"].sum() if not funnel_rt.empty else 0)))

        ordered_bar_chart(
            funnel_rt,
            x="event_name",
            y="count",
            title="Funnel realtime",
            order=FUNNEL_EVENTS,
            height=360,
        )

        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Tabla funnel")
            tbl = funnel_rt.copy()
            if "share" not in tbl.columns:
                tbl = add_share(tbl, "count", "share")
            tbl["share_pct"] = (tbl["share"] * 100.0).round(2)
            st.dataframe(tbl, use_container_width=True, height=320)

        with c2:
            st.subheader("Top productos por purchase")
            tbl = purchase_rt.copy()
            if not tbl.empty:
                tbl["share_pct"] = (tbl["share"] * 100.0).round(2)
            st.dataframe(tbl, use_container_width=True, height=320)

    with rt_tabs[2]:
        bar_chart(
            by_event,
            x="event_name",
            y="count",
            title="Mix de eventos (Realtime)",
            top_n=top_n,
            height=420,
        )

        st.divider()
        c1, c2 = st.columns(2)

        with c1:
            pareto_chart(
                by_event,
                category="event_name",
                value="count",
                title="Pareto de eventos",
                top_n=min(30, max(10, top_n)),
                height=340,
            )

        with c2:
            st.subheader("Tabla de eventos")
            tbl = by_event.copy()
            if not tbl.empty:
                tbl["share_pct"] = (tbl["share"] * 100.0).round(2)
                tbl["avg_per_active_product"] = tbl["avg_per_active_product"].round(2)
            st.dataframe(tbl, use_container_width=True, height=340)

    with rt_tabs[3]:
        bar_chart(
            by_product,
            x="product_id",
            y="count",
            title="Productos con mayor actividad",
            top_n=top_n,
            height=420,
        )

        st.divider()
        c1, c2 = st.columns(2)

        with c1:
            pareto_chart(
                by_product,
                category="product_id",
                value="count",
                title="Pareto de productos",
                top_n=min(30, max(10, top_n)),
                height=340,
            )

        with c2:
            st.subheader("Tabla de actividad por producto")
            tbl = by_product.copy()
            if not tbl.empty:
                tbl["share_pct"] = (tbl["share"] * 100.0).round(2)
            st.dataframe(tbl, use_container_width=True, height=340)

    with rt_tabs[4]:
        if df_rt.empty:
            st.info("No hay datos para drilldown.")
        else:
            options = by_product["product_id"].head(200).tolist()
            default_prod = product_filter if (product_filter in options) else (options[0] if options else None)

            selected = st.selectbox(
                "Selecciona product_id",
                options=options,
                index=options.index(default_prod) if default_prod in options else 0,
            )

            df_p = df_rt[df_rt["product_id"] == selected].copy()
            df_p = df_p.groupby("event_name", as_index=False)["count"].sum().sort_values("count", ascending=False)
            df_p = add_share(df_p, "count", "share")

            total_p = int(df_p["count"].sum()) if not df_p.empty else 0
            event_types_p = int(df_p["event_name"].nunique()) if not df_p.empty else 0
            purchase_share_p = _fmt_pct(_ratio(_value_from_df(df_p, "event_name", "purchase", "count"), total_p))

            c1, c2 = st.columns([2, 1])
            with c1:
                bar_chart(
                    df_p,
                    x="event_name",
                    y="count",
                    title="Mix de eventos del producto seleccionado",
                    top_n=top_n,
                    height=420,
                )
            with c2:
                st.metric("Eventos del producto", _fmt_int(total_p))
                st.metric("Tipos de evento", _fmt_int(event_types_p))
                st.metric("Peso purchase", purchase_share_p)
                tbl = df_p.copy()
                if not tbl.empty:
                    tbl["share_pct"] = (tbl["share"] * 100.0).round(2)
                st.dataframe(tbl, use_container_width=True, height=420)


# -----------------------------
# TAB: Historico (Hive)
# -----------------------------
with tab_hist:
    st.subheader("Historico (Batch Layer) - Hive")
    st.caption(
        "Aquí sí se aprovecha el esquema rico: revenue, order, marketing, customer, device, geo y product."
    )

    hist_tabs = st.tabs(
        [
            "Resumen ejecutivo",
            "Mix y funnel",
            "Marketing y clientes",
            "Productos",
            "Operacion y tiempo",
        ]
    )

    # 1) Resumen ejecutivo
    with hist_tabs[0]:
        sql_default = sql_overview(hive_table, start_sql, end_sql)
        with st.expander("SQL overview (editable)", expanded=True):
            sql = st.text_area("SQL", value=sql_default, height=220, key="sql_overview")

        run = st.button("Ejecutar overview", use_container_width=True, key="run_overview")
        if run:
            try:
                with st.spinner("Consultando Hive..."):
                    df = cached_hive_query(sql)

                st.success(f"OK: {len(df)} fila(s)")
                st.dataframe(df, use_container_width=True, height=160)

                if not df.empty:
                    row = df.iloc[0].to_dict()

                    total_events = int(row.get("total_events", 0) or 0)
                    users = int(row.get("users", 0) or 0)
                    sessions = int(row.get("sessions", 0) or 0)
                    customers = int(row.get("customers", 0) or 0)

                    purchases = int(row.get("purchase", 0) or 0)
                    orders = int(row.get("distinct_orders", 0) or 0)
                    buyers = int(row.get("buyers", 0) or 0)
                    revenue = float(row.get("realized_revenue_usd", 0.0) or 0.0)
                    aov = float(row.get("avg_order_value_usd", 0.0) or 0.0)

                    views = int(row.get("product_view", 0) or 0)
                    carts = int(row.get("add_to_cart", 0) or 0)
                    checkouts = int(row.get("begin_checkout", 0) or 0)

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Eventos", _fmt_int(total_events))
                    c2.metric("Usuarios", _fmt_int(users))
                    c3.metric("Sesiones", _fmt_int(sessions))
                    c4.metric("Customers", _fmt_int(customers))

                    c5, c6, c7, c8 = st.columns(4)
                    c5.metric("Purchase events", _fmt_int(purchases))
                    c6.metric("Órdenes", _fmt_int(orders))
                    c7.metric("Buyers", _fmt_int(buyers))
                    c8.metric("Revenue", _fmt_money(revenue))

                    c9, c10, c11, c12 = st.columns(4)
                    c9.metric("View → Cart", _fmt_pct(_ratio(carts, views)))
                    c10.metric("Cart → Checkout", _fmt_pct(_ratio(checkouts, carts)))
                    c11.metric("Checkout → Purchase", _fmt_pct(_ratio(purchases, checkouts)))
                    c12.metric("AOV", _fmt_money(aov))

                    funnel_df = pd.DataFrame(
                        [
                            {"event_name": evt, "count": int(row.get(evt, 0) or 0)}
                            for evt in FUNNEL_EVENTS
                        ]
                    )
                    ordered_bar_chart(
                        funnel_df,
                        x="event_name",
                        y="count",
                        title="Funnel histórico",
                        order=FUNNEL_EVENTS,
                        height=360,
                    )

            except Exception as e:
                st.warning(f"Hive no disponible / tabla no existe todavia: {e}")

    # 2) Mix y funnel
    with hist_tabs[1]:
        c1, c2 = st.columns(2)

        with c1:
            sql_default = sql_event_mix(hive_table, start_sql, end_sql)
            with st.expander("SQL mix de eventos", expanded=True):
                sql = st.text_area("SQL", value=sql_default, height=200, key="sql_mix_events")

            run = st.button("Ejecutar mix de eventos", use_container_width=True, key="run_mix_events")
            if run:
                try:
                    with st.spinner("Consultando Hive..."):
                        df = cached_hive_query(sql)

                    st.success(f"OK: {len(df)} filas")
                    if {"event_name", "events"}.issubset(df.columns):
                        df2 = add_share(df.copy(), "events", "share")
                        bar_chart(df2, x="event_name", y="events", title="Mix histórico por evento", top_n=top_n, height=380)

                        tbl = df2.copy()
                        tbl["share_pct"] = (tbl["share"] * 100.0).round(2)
                        st.dataframe(tbl, use_container_width=True, height=300)
                    else:
                        st.dataframe(df, use_container_width=True, height=300)

                except Exception as e:
                    st.warning(f"Hive no disponible / tabla no existe todavia: {e}")

        with c2:
            sql_default = sql_daily_series(hive_table, start_sql, end_sql)
            with st.expander("SQL serie diaria", expanded=True):
                sql = st.text_area("SQL", value=sql_default, height=200, key="sql_daily_series")

            run = st.button("Ejecutar serie diaria", use_container_width=True, key="run_daily_series")
            if run:
                try:
                    with st.spinner("Consultando Hive..."):
                        df = cached_hive_query(sql)

                    st.success(f"OK: {len(df)} filas")
                    if not df.empty and "time_bucket" in df.columns:
                        df["time_bucket"] = pd.to_datetime(df["time_bucket"], errors="coerce", utc=True)
                        df = df.dropna(subset=["time_bucket"]).sort_values("time_bucket")

                        total_rev = float(pd.to_numeric(df.get("revenue_usd", 0), errors="coerce").fillna(0).sum())
                        total_orders = int(pd.to_numeric(df.get("orders", 0), errors="coerce").fillna(0).sum())
                        avg_daily_rev = total_rev / len(df) if len(df) else 0.0
                        peak_day_rev = float(pd.to_numeric(df.get("revenue_usd", 0), errors="coerce").fillna(0).max()) if "revenue_usd" in df.columns else 0.0

                        r1, r2, r3, r4 = st.columns(4)
                        r1.metric("Dias", _fmt_int(len(df)))
                        r2.metric("Revenue total", _fmt_money(total_rev))
                        r3.metric("Órdenes", _fmt_int(total_orders))
                        r4.metric("Revenue promedio / día", _fmt_money(avg_daily_rev))

                        if "revenue_usd" in df.columns:
                            line_chart(df, x="time_bucket", y="revenue_usd", title="Revenue por día", height=250)
                        if "orders" in df.columns:
                            line_chart(df, x="time_bucket", y="orders", title="Órdenes por día", height=250)

                        st.dataframe(df, use_container_width=True, height=240)

                except Exception as e:
                    st.warning(f"Hive no disponible / tabla no existe todavia: {e}")

    # 3) Marketing y clientes
    with hist_tabs[2]:
        c1, c2 = st.columns(2)

        with c1:
            marketing_dim = st.selectbox("Dimensión marketing", list(MARKETING_DIMENSIONS.keys()), key="marketing_dim")
            metric_marketing = st.selectbox("Métrica marketing", ["revenue_usd", "orders", "sessions", "users", "events"], key="marketing_metric")

            expr, purchase_only = MARKETING_DIMENSIONS[marketing_dim]
            sql_default = sql_dimension_generic(hive_table, start_sql, end_sql, expr, purchase_only=purchase_only)
            with st.expander("SQL marketing", expanded=True):
                sql = st.text_area("SQL", value=sql_default, height=180, key="sql_marketing")

            run = st.button("Ejecutar marketing", use_container_width=True, key="run_marketing")
            if run:
                try:
                    with st.spinner("Consultando Hive..."):
                        df = cached_hive_query(sql)

                    st.success(f"OK: {len(df)} filas")
                    if {"dimension", metric_marketing}.issubset(df.columns):
                        df2 = add_share(df.copy(), metric_marketing, "share")
                        bar_chart(df2, x="dimension", y=metric_marketing, title=f"{marketing_dim} por {metric_marketing}", top_n=top_n, height=380)
                        out = df2.copy()
                        out["share_pct"] = (out["share"] * 100.0).round(2)
                        st.dataframe(out, use_container_width=True, height=280)
                    else:
                        st.dataframe(df, use_container_width=True, height=280)

                except Exception as e:
                    st.warning(f"Hive no disponible / tabla no existe todavia: {e}")

        with c2:
            customer_dim = st.selectbox("Dimensión cliente", list(CUSTOMER_DIMENSIONS.keys()), key="customer_dim")
            metric_customer = st.selectbox("Métrica cliente", ["revenue_usd", "orders", "buyers", "users", "events"], key="customer_metric")

            expr, purchase_only = CUSTOMER_DIMENSIONS[customer_dim]
            sql_default = sql_dimension_generic(hive_table, start_sql, end_sql, expr, purchase_only=purchase_only)
            with st.expander("SQL clientes", expanded=True):
                sql = st.text_area("SQL", value=sql_default, height=180, key="sql_customer")

            run = st.button("Ejecutar clientes", use_container_width=True, key="run_customer")
            if run:
                try:
                    with st.spinner("Consultando Hive..."):
                        df = cached_hive_query(sql)

                    st.success(f"OK: {len(df)} filas")
                    if {"dimension", metric_customer}.issubset(df.columns):
                        df2 = add_share(df.copy(), metric_customer, "share")
                        bar_chart(df2, x="dimension", y=metric_customer, title=f"{customer_dim} por {metric_customer}", top_n=top_n, height=380)
                        out = df2.copy()
                        out["share_pct"] = (out["share"] * 100.0).round(2)
                        st.dataframe(out, use_container_width=True, height=280)
                    else:
                        st.dataframe(df, use_container_width=True, height=280)

                except Exception as e:
                    st.warning(f"Hive no disponible / tabla no existe todavia: {e}")

    # 4) Productos
    with hist_tabs[3]:
        product_dim = st.selectbox("Dimensión producto", list(PRODUCT_DIMENSIONS.keys()), key="product_dim")
        metric_product = st.selectbox(
            "Métrica producto",
            ["revenue_usd", "orders", "buyers", "purchase_events", "begin_checkout", "add_to_cart", "product_views"],
            key="product_metric",
        )

        expr = PRODUCT_DIMENSIONS[product_dim]
        sql_default = sql_product_dimension(hive_table, start_sql, end_sql, expr)
        with st.expander("SQL productos", expanded=True):
            sql = st.text_area("SQL", value=sql_default, height=200, key="sql_product")

        run = st.button("Ejecutar productos", use_container_width=True, key="run_product")
        if run:
            try:
                with st.spinner("Consultando Hive..."):
                    df = cached_hive_query(sql)

                st.success(f"OK: {len(df)} filas")
                if {"dimension", metric_product}.issubset(df.columns):
                    df2 = add_share(df.copy(), metric_product, "share")
                    bar_chart(df2, x="dimension", y=metric_product, title=f"{product_dim} por {metric_product}", top_n=top_n, height=420)

                    total_views = int(pd.to_numeric(df2.get("product_views", 0), errors="coerce").fillna(0).sum())
                    total_carts = int(pd.to_numeric(df2.get("add_to_cart", 0), errors="coerce").fillna(0).sum())
                    total_orders = int(pd.to_numeric(df2.get("orders", 0), errors="coerce").fillna(0).sum())
                    total_rev = float(pd.to_numeric(df2.get("revenue_usd", 0), errors="coerce").fillna(0).sum())

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Product views", _fmt_int(total_views))
                    c2.metric("Add to cart", _fmt_int(total_carts))
                    c3.metric("Órdenes", _fmt_int(total_orders))
                    c4.metric("Revenue", _fmt_money(total_rev))

                    out = df2.copy()
                    out["share_pct"] = (out["share"] * 100.0).round(2)
                    st.dataframe(out, use_container_width=True, height=320)
                else:
                    st.dataframe(df, use_container_width=True, height=320)

            except Exception as e:
                st.warning(f"Hive no disponible / tabla no existe todavia: {e}")

    # 5) Operación y tiempo
    with hist_tabs[4]:
        top_l, top_r = st.columns(2)

        with top_l:
            ops_dim = st.selectbox("Dimensión operativa", list(OPS_DIMENSIONS.keys()), key="ops_dim")
            metric_ops = st.selectbox("Métrica operativa", ["revenue_usd", "orders", "users", "sessions", "events"], key="ops_metric")

            expr, purchase_only = OPS_DIMENSIONS[ops_dim]
            sql_default = sql_dimension_generic(hive_table, start_sql, end_sql, expr, purchase_only=purchase_only)
            with st.expander("SQL operación", expanded=True):
                sql = st.text_area("SQL", value=sql_default, height=180, key="sql_ops")

            run = st.button("Ejecutar operación", use_container_width=True, key="run_ops")
            if run:
                try:
                    with st.spinner("Consultando Hive..."):
                        df = cached_hive_query(sql)

                    st.success(f"OK: {len(df)} filas")
                    if {"dimension", metric_ops}.issubset(df.columns):
                        df2 = add_share(df.copy(), metric_ops, "share")
                        bar_chart(df2, x="dimension", y=metric_ops, title=f"{ops_dim} por {metric_ops}", top_n=top_n, height=360)
                        out = df2.copy()
                        out["share_pct"] = (out["share"] * 100.0).round(2)
                        st.dataframe(out, use_container_width=True, height=260)
                    else:
                        st.dataframe(df, use_container_width=True, height=260)

                except Exception as e:
                    st.warning(f"Hive no disponible / tabla no existe todavia: {e}")

        with top_r:
            sql_default = sql_hour_distribution(hive_table, start_sql, end_sql)
            with st.expander("SQL distribución por hora", expanded=True):
                sql = st.text_area("SQL", value=sql_default, height=180, key="sql_hour_dist")

            run = st.button("Ejecutar distribución por hora", use_container_width=True, key="run_hour_dist")
            if run:
                try:
                    with st.spinner("Consultando Hive..."):
                        df = cached_hive_query(sql)

                    st.success(f"OK: {len(df)} filas")
                    if {"hour_of_day", "cnt"}.issubset(df.columns):
                        df2 = df.copy()
                        df2["hour_of_day"] = pd.to_numeric(df2["hour_of_day"], errors="coerce")
                        df2 = df2.dropna(subset=["hour_of_day"]).sort_values("hour_of_day")
                        bar_chart(df2, x="hour_of_day", y="cnt", title="Eventos por hora del día", top_n=24, height=360)

                    st.dataframe(df, use_container_width=True, height=260)

                except Exception as e:
                    st.warning(f"Hive no disponible / tabla no existe todavia: {e}")

        st.divider()

        sql_default = sql_heatmap_event_day(hive_table, start_sql, end_sql)
        with st.expander("SQL heatmap día vs evento", expanded=True):
            sql = st.text_area("SQL", value=sql_default, height=180, key="sql_heatmap_event_day")

        run = st.button("Ejecutar heatmap día vs evento", use_container_width=True, key="run_heatmap_event_day")
        if run:
            try:
                with st.spinner("Consultando Hive..."):
                    df = cached_hive_query(sql)

                st.success(f"OK: {len(df)} filas")
                if {"time_bucket", "event_name", "cnt"}.issubset(df.columns):
                    top_events = (
                        df.groupby("event_name", as_index=False)["cnt"]
                        .sum()
                        .sort_values("cnt", ascending=False)
                        .head(min(15, max(8, top_n)))["event_name"]
                        .tolist()
                    )
                    df2 = df[df["event_name"].isin(top_events)].copy()
                    heatmap_chart(
                        df2,
                        x="event_name",
                        y="time_bucket",
                        value="cnt",
                        title="Mix diario por evento",
                        height=520,
                    )

                st.dataframe(df, use_container_width=True, height=260)

            except Exception as e:
                st.warning(f"Hive no disponible / tabla no existe todavia: {e}")


# -----------------------------
# TAB: Lambda View (Batch + Speed)
# -----------------------------
with tab_lambda:
    st.subheader("Lambda View (Batch + Speed)")
    st.caption(serving.lambda_view_note())

    c1, c2 = st.columns(2)
    c1.metric("Rango (inicio)", start_iso)
    c2.metric("Rango (fin)", end_iso)

    st.divider()

    sql_batch_default = sql_event_mix(hive_table, start_sql, end_sql)
    with st.expander("SQL Batch (Hive)", expanded=True):
        sql_batch_user = st.text_area("SQL", value=sql_batch_default, height=180, key="sql_lambda")

    run_mix = st.button("Ejecutar comparación (Hive + Redis)", use_container_width=True, key="run_lambda")

    if run_mix:
        df_batch = pd.DataFrame()
        df_rt2 = pd.DataFrame()

        left, right = st.columns(2)

        with left:
            st.subheader("Batch (Hive)")
            try:
                with st.spinner("Consultando Hive..."):
                    df_batch = cached_hive_query(sql_batch_user)
                st.success(f"Batch OK: {len(df_batch)} filas")
                st.dataframe(df_batch, use_container_width=True, height=320)
            except Exception as e:
                st.warning(f"Batch (Hive) no listo todavía: {e}")

        with right:
            st.subheader("Speed (Redis)")
            try:
                df_rt2 = cached_realtime_stats(product_filter)
                st.success(f"Speed OK: {len(df_rt2)} filas (snapshot)")
                st.dataframe(df_rt2, use_container_width=True, height=320)
            except Exception as e:
                st.warning(f"Speed (Redis) no listo todavía: {e}")

        st.divider()

        batch_by_event = batch_event_counts(df_batch)

        if not df_rt2.empty and {"event_name", "count"}.issubset(df_rt2.columns):
            rt_by_event = (
                df_rt2.groupby("event_name", as_index=False)["count"]
                .sum()
                .rename(columns={"count": "realtime_cnt"})
            )
            rt_by_event = add_share(rt_by_event, "realtime_cnt", "realtime_share")
        else:
            rt_by_event = pd.DataFrame(columns=["event_name", "realtime_cnt", "realtime_share"])

        merged = pd.merge(batch_by_event, rt_by_event, on="event_name", how="outer").fillna(0)

        if merged.empty:
            st.info("No hay suficiente información para comparar.")
        else:
            merged["batch_cnt"] = _safe_int_series(merged["batch_cnt"])
            merged["realtime_cnt"] = _safe_int_series(merged["realtime_cnt"])
            merged["batch_share"] = _safe_float_series(merged["batch_share"])
            merged["realtime_share"] = _safe_float_series(merged["realtime_share"])

            merged["delta_speed_minus_batch"] = merged["realtime_cnt"] - merged["batch_cnt"]
            merged["share_delta_pp"] = (merged["realtime_share"] - merged["batch_share"]) * 100.0

            batch_purchase_share = 0.0
            rt_purchase_share = 0.0
            if "purchase" in merged["event_name"].tolist():
                tmp = merged[merged["event_name"] == "purchase"].iloc[0]
                batch_purchase_share = float(tmp["batch_share"])
                rt_purchase_share = float(tmp["realtime_share"])

            cA, cB, cC, cD = st.columns(4)
            cA.metric("Eventos Batch", _fmt_int(merged["batch_cnt"].sum()))
            cB.metric("Eventos Speed", _fmt_int(merged["realtime_cnt"].sum()))
            cC.metric("Peso purchase Batch", _fmt_pct(batch_purchase_share))
            cD.metric("Peso purchase Speed", _fmt_pct(rt_purchase_share))

            grouped_compare_chart(
                merged,
                category="event_name",
                a="batch_cnt",
                b="realtime_cnt",
                title="Comparación por evento",
                top_n=top_n,
                height=480,
            )

            mix_tbl = merged.copy().sort_values("share_delta_pp", ascending=False)
            mix_tbl["batch_share_pct"] = (mix_tbl["batch_share"] * 100.0).round(2)
            mix_tbl["realtime_share_pct"] = (mix_tbl["realtime_share"] * 100.0).round(2)
            mix_tbl["share_delta_pp"] = mix_tbl["share_delta_pp"].round(2)

            st.subheader("Cambio de composición (puntos porcentuales)")
            st.dataframe(
                mix_tbl[
                    [
                        "event_name",
                        "batch_cnt",
                        "realtime_cnt",
                        "delta_speed_minus_batch",
                        "batch_share_pct",
                        "realtime_share_pct",
                        "share_delta_pp",
                    ]
                ],
                use_container_width=True,
                height=420,
            )
