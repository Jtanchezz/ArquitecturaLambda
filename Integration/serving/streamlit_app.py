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
    page_title="Lambda Architecture - Serving",
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
# Helpers
# -----------------------------
def _fmt_int(n) -> str:
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return str(n)


def _safe_int_series(s: pd.Series) -> pd.Series:
    try:
        return s.fillna(0).astype("int64")
    except Exception:
        return pd.to_numeric(s, errors="coerce").fillna(0).astype("int64")


def _rerun():
    # compat
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()


def kpis_from_rt(df_rt: pd.DataFrame) -> dict:
    if df_rt is None or df_rt.empty:
        return {"total": 0, "products": 0, "events": 0, "avg_per_product": 0}

    total = int(df_rt["count"].sum())
    products = int(df_rt["product_id"].nunique())
    events = int(df_rt["event_name"].nunique())
    avg_per_product = int(total / products) if products else 0
    return {"total": total, "products": products, "events": events, "avg_per_product": avg_per_product}


def kpis_from_batch(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {"rows": 0, "total": 0, "series": 0}

    numeric_cols = [c for c in df.columns if c.lower() in ("cnt", "count", "counts")]
    total = int(df[numeric_cols[0]].sum()) if numeric_cols else 0

    series = 0
    for c in ("event_name", "time_bucket", "hour_of_day", "dow"):
        if c in df.columns:
            series = int(df[c].nunique())
            break

    return {"rows": int(len(df)), "total": total, "series": series}


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

    chart = (
        alt.Chart(dfp)
        .mark_bar()
        .encode(
            x=alt.X(f"{y}:Q", title=y),
            y=alt.Y(f"{x}:N", sort="-x", title=""),
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
    dfp[value] = _safe_int_series(dfp[value])
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
            tooltip=[alt.Tooltip(f"{category}:N"), alt.Tooltip(f"{value}:Q"), alt.Tooltip("cumulative_pct:Q")],
        )
    )

    line = (
        alt.Chart(dfp)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{category}:N", sort=None),
            y=alt.Y("cumulative_pct:Q", title="Acumulado (%)"),
            tooltip=[alt.Tooltip("cumulative_pct:Q")],
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

    melted = dfp[[category, a, b]].melt(id_vars=[category], var_name="layer", value_name="count")
    chart = (
        alt.Chart(melted)
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="count"),
            y=alt.Y(f"{category}:N", sort="-x", title=""),
            color="layer:N",
            tooltip=[alt.Tooltip(f"{category}:N"), alt.Tooltip("layer:N"), alt.Tooltip("count:Q")],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


# Cache Hive
@st.cache_data(ttl=60, show_spinner=False)
def cached_hive_query(sql: str) -> pd.DataFrame:
    return serving.hive_query(sql)


# Cache Redis (para que el boton "Actualizar" tenga efecto real)
@st.cache_data(ttl=5, show_spinner=False)
def cached_realtime_stats(product_id: str | None) -> pd.DataFrame:
    return serving.get_realtime_stats(product_id=product_id)


# -----------------------------
# Header
# -----------------------------
st.title("Serving Layer - Lambda Architecture")
st.caption("Dashboards para Speed (Redis), Batch (Hive) y comparacion (Lambda View)")


# -----------------------------
# Sidebar (filtros + actualizar global)
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

    st.divider()

    if st.button("Actualizar", use_container_width=True):
        cached_hive_query.clear()
        cached_realtime_stats.clear()
        _rerun()

    st.caption("En Hive puedes editar el SQL dentro de cada dashboard.")


tab_rt, tab_hist, tab_lambda = st.tabs(
    ["Realtime (Redis)", "Historico (Hive)", "Lambda View (Batch + Speed)"]
)


# -----------------------------
# TAB: Realtime (Redis) - 5 dashboards
# -----------------------------
with tab_rt:
    topbar_l, topbar_r = st.columns([1, 3])
    with topbar_l:
        if st.button("Actualizar realtime", use_container_width=True):
            cached_realtime_stats.clear()
            _rerun()
    with topbar_r:
        st.caption("Snapshot acumulado por producto y tipo de evento (sin ventana temporal).")

    try:
        df_rt = cached_realtime_stats(product_filter)
    except Exception as e:
        df_rt = pd.DataFrame(columns=["product_id", "event_name", "count"])
        st.warning(f"Redis no disponible o sin datos: {e}")

    rt_tabs = st.tabs(
        [
            "Resumen",
            "Top eventos",
            "Top productos",
            "Drilldown por producto",
            "Distribucion y matriz",
        ]
    )

    if df_rt is None or df_rt.empty:
        by_event = pd.DataFrame(columns=["event_name", "count"])
        by_product = pd.DataFrame(columns=["product_id", "count"])
    else:
        by_event = df_rt.groupby("event_name", as_index=False)["count"].sum().sort_values("count", ascending=False)
        by_product = df_rt.groupby("product_id", as_index=False)["count"].sum().sort_values("count", ascending=False)

    # 1) Resumen
    with rt_tabs[0]:
        k = kpis_from_rt(df_rt)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Eventos totales (sum)", _fmt_int(k["total"]))
        c2.metric("Productos (n)", _fmt_int(k["products"]))
        c3.metric("Tipos de evento (n)", _fmt_int(k["events"]))
        c4.metric("Promedio por producto", _fmt_int(k["avg_per_product"]))

        st.divider()
        st.subheader("Detalle (snapshot)")
        st.dataframe(df_rt, use_container_width=True, height=460)

        csv = df_rt.to_csv(index=False).encode("utf-8")
        st.download_button("Descargar CSV (Realtime)", csv, "realtime_snapshot.csv", "text/csv")

    # 2) Top eventos
    with rt_tabs[1]:
        bar_chart(by_event, x="event_name", y="count", title="Top eventos (Realtime)", top_n=top_n, height=460)
        st.divider()
        st.subheader("Tabla")
        st.dataframe(by_event.head(max(top_n, 25)), use_container_width=True, height=420)

    # 3) Top productos
    with rt_tabs[2]:
        bar_chart(by_product, x="product_id", y="count", title="Top productos (Realtime)", top_n=top_n, height=460)
        st.divider()
        st.subheader("Tabla")
        st.dataframe(by_product.head(max(top_n, 25)), use_container_width=True, height=420)

    # 4) Drilldown por producto
    with rt_tabs[3]:
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

            c1, c2 = st.columns([2, 1])
            with c1:
                bar_chart(df_p, x="event_name", y="count", title="Eventos para el producto seleccionado", top_n=top_n, height=420)
            with c2:
                total_p = int(df_p["count"].sum()) if not df_p.empty else 0
                st.metric("Total del producto (sum)", _fmt_int(total_p))
                st.dataframe(df_p, use_container_width=True, height=420)

    # 5) Distribucion y matriz
    with rt_tabs[4]:
        if df_rt.empty:
            st.info("No hay datos para distribucion.")
        else:
            c1, c2 = st.columns(2)
            with c1:
                pareto_chart(by_event, category="event_name", value="count", title="Pareto de eventos (Realtime)", top_n=min(30, max(10, top_n)), height=360)
            with c2:
                pareto_chart(by_product, category="product_id", value="count", title="Pareto de productos (Realtime)", top_n=min(30, max(10, top_n)), height=360)

            st.divider()

            top_products = by_product.head(min(20, max(10, top_n)))["product_id"].tolist()
            top_events = by_event.head(min(15, max(8, top_n)))["event_name"].tolist()

            df_hm = df_rt[df_rt["product_id"].isin(top_products) & df_rt["event_name"].isin(top_events)].copy()
            df_hm = df_hm.groupby(["product_id", "event_name"], as_index=False)["count"].sum()

            heatmap_chart(
                df_hm,
                x="event_name",
                y="product_id",
                value="count",
                title="Matriz producto vs evento (subset Top)",
                height=520,
            )


# -----------------------------
# TAB: Historico (Hive) - 5 dashboards
# -----------------------------
with tab_hist:
    st.subheader("Historico (Batch Layer) - Hive")
    st.caption(f"Rango: {start_iso} -> {end_iso}")

    t = SETTINGS.hive_events_table
    start_sql = start_iso.replace("T", " ").replace("Z", "")
    end_sql = end_iso.replace("T", " ").replace("Z", "")

    hist_tabs = st.tabs(
        [
            "Ranking de eventos",
            "Serie temporal por hora",
            "Serie temporal por dia",
            "Heatmap dia vs evento",
            "Distribucion por hora",
        ]
    )

    # 1) ranking por evento
    with hist_tabs[0]:
        sql_default = serving.default_hive_sql(start_iso, end_iso)
        with st.expander("SQL (editable)", expanded=True):
            sql = st.text_area("SQL", value=sql_default, height=160, key="sql_rank")

        run = st.button("Ejecutar dashboard", use_container_width=True, key="run_rank")
        if run:
            try:
                with st.spinner("Consultando Hive..."):
                    df = cached_hive_query(sql)

                st.success(f"OK: {len(df)} filas")
                k = kpis_from_batch(df)
                c1, c2, c3 = st.columns(3)
                c1.metric("Filas", _fmt_int(k["rows"]))
                c2.metric("Total (sum cnt)", _fmt_int(k["total"]))
                c3.metric("Categorias", _fmt_int(k["series"]))

                if "event_name" in df.columns and "cnt" in df.columns:
                    bar_chart(df, x="event_name", y="cnt", title="Ranking historico por evento", top_n=top_n, height=460)

                st.divider()
                st.dataframe(df, use_container_width=True, height=420)

                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("Descargar CSV (Hive)", csv, "hive_rank_eventos.csv", "text/csv", key="dl_rank")

            except Exception as e:
                st.warning(f"Hive no disponible / tabla no existe todavia (OK por ahora): {e}")

    # 2) serie por hora
    with hist_tabs[1]:
        sql_default = f"""
SELECT
  from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd HH:00:00') AS time_bucket,
  COUNT(*) AS cnt
FROM {t}
WHERE event_time >= TIMESTAMP '{start_sql}'
  AND event_time <  TIMESTAMP '{end_sql}'
GROUP BY from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd HH:00:00')
ORDER BY time_bucket ASC
"""
        with st.expander("SQL (editable)", expanded=True):
            sql = st.text_area("SQL", value=sql_default, height=160, key="sql_hour")

        run = st.button("Ejecutar dashboard", use_container_width=True, key="run_hour")
        if run:
            try:
                with st.spinner("Consultando Hive..."):
                    df = cached_hive_query(sql)

                st.success(f"OK: {len(df)} filas")
                k = kpis_from_batch(df)
                c1, c2, c3 = st.columns(3)
                c1.metric("Filas", _fmt_int(k["rows"]))
                c2.metric("Total (sum cnt)", _fmt_int(k["total"]))
                c3.metric("Buckets", _fmt_int(k["series"]))

                if "time_bucket" in df.columns and "cnt" in df.columns:
                    df2 = df.copy()
                    df2["time_bucket"] = pd.to_datetime(df2["time_bucket"], errors="coerce", utc=True)
                    df2 = df2.dropna(subset=["time_bucket"]).sort_values("time_bucket")
                    line_chart(df2, x="time_bucket", y="cnt", title="Serie temporal por hora", height=380)

                st.divider()
                st.dataframe(df, use_container_width=True, height=420)

                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("Descargar CSV (Hive)", csv, "hive_serie_hora.csv", "text/csv", key="dl_hour")

            except Exception as e:
                st.warning(f"Hive no disponible / tabla no existe todavia (OK por ahora): {e}")

    # 3) serie por dia
    with hist_tabs[2]:
        sql_default = f"""
SELECT
  from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd') AS time_bucket,
  COUNT(*) AS cnt
FROM {t}
WHERE event_time >= TIMESTAMP '{start_sql}'
  AND event_time <  TIMESTAMP '{end_sql}'
GROUP BY from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd')
ORDER BY time_bucket ASC
"""
        with st.expander("SQL (editable)", expanded=True):
            sql = st.text_area("SQL", value=sql_default, height=160, key="sql_day")

        run = st.button("Ejecutar dashboard", use_container_width=True, key="run_day")
        if run:
            try:
                with st.spinner("Consultando Hive..."):
                    df = cached_hive_query(sql)

                st.success(f"OK: {len(df)} filas")
                k = kpis_from_batch(df)
                c1, c2, c3 = st.columns(3)
                c1.metric("Filas", _fmt_int(k["rows"]))
                c2.metric("Total (sum cnt)", _fmt_int(k["total"]))
                c3.metric("Buckets", _fmt_int(k["series"]))

                if "time_bucket" in df.columns and "cnt" in df.columns:
                    df2 = df.copy()
                    df2["time_bucket"] = pd.to_datetime(df2["time_bucket"], errors="coerce", utc=True)
                    df2 = df2.dropna(subset=["time_bucket"]).sort_values("time_bucket")
                    line_chart(df2, x="time_bucket", y="cnt", title="Serie temporal por dia", height=380)

                st.divider()
                st.dataframe(df, use_container_width=True, height=420)

                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("Descargar CSV (Hive)", csv, "hive_serie_dia.csv", "text/csv", key="dl_day")

            except Exception as e:
                st.warning(f"Hive no disponible / tabla no existe todavia (OK por ahora): {e}")

    # 4) heatmap dia vs evento
    with hist_tabs[3]:
        sql_default = f"""
SELECT
  from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd') AS time_bucket,
  event_name,
  COUNT(*) AS cnt
FROM {t}
WHERE event_time >= TIMESTAMP '{start_sql}'
  AND event_time <  TIMESTAMP '{end_sql}'
GROUP BY from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd'), event_name
"""
        with st.expander("SQL (editable)", expanded=True):
            sql = st.text_area("SQL", value=sql_default, height=180, key="sql_hm")

        run = st.button("Ejecutar dashboard", use_container_width=True, key="run_hm")
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
                        title="Heatmap dia vs evento (Top eventos)",
                        height=520,
                    )

                st.divider()
                st.dataframe(df, use_container_width=True, height=420)

                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("Descargar CSV (Hive)", csv, "hive_heatmap_dia_evento.csv", "text/csv", key="dl_hm")

            except Exception as e:
                st.warning(f"Hive no disponible / tabla no existe todavia (OK por ahora): {e}")

    # 5) distribucion por hora del dia
    with hist_tabs[4]:
        sql_default = f"""
SELECT
  hour(event_time) AS hour_of_day,
  COUNT(*) AS cnt
FROM {t}
WHERE event_time >= TIMESTAMP '{start_sql}'
  AND event_time <  TIMESTAMP '{end_sql}'
GROUP BY hour(event_time)
ORDER BY hour_of_day ASC
"""
        with st.expander("SQL (editable)", expanded=True):
            sql = st.text_area("SQL", value=sql_default, height=160, key="sql_hod")

        run = st.button("Ejecutar dashboard", use_container_width=True, key="run_hod")
        if run:
            try:
                with st.spinner("Consultando Hive..."):
                    df = cached_hive_query(sql)

                st.success(f"OK: {len(df)} filas")
                if {"hour_of_day", "cnt"}.issubset(df.columns):
                    df2 = df.copy()
                    df2["hour_of_day"] = pd.to_numeric(df2["hour_of_day"], errors="coerce")
                    df2 = df2.dropna(subset=["hour_of_day"]).sort_values("hour_of_day")
                    bar_chart(df2, x="hour_of_day", y="cnt", title="Distribucion por hora del dia", top_n=24, height=420)

                st.divider()
                st.dataframe(df, use_container_width=True, height=420)

                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("Descargar CSV (Hive)", csv, "hive_distribucion_hora.csv", "text/csv", key="dl_hod")

            except Exception as e:
                st.warning(f"Hive no disponible / tabla no existe todavia (OK por ahora): {e}")


# -----------------------------
# TAB: Lambda View (Batch + Speed)
# -----------------------------
with tab_lambda:
    st.subheader("Lambda View (Batch + Speed)")
    st.caption("Comparacion por event_name: Batch (Hive) vs Speed (Redis).")

    c1, c2 = st.columns(2)
    c1.metric("Rango (inicio)", start_iso)
    c2.metric("Rango (fin)", end_iso)

    st.divider()

    sql_batch_default = serving.default_hive_sql(start_iso, end_iso)
    with st.expander("SQL Batch (Hive)", expanded=True):
        sql_batch_user = st.text_area("SQL", value=sql_batch_default, height=160)

    run_mix = st.button("Ejecutar comparacion (Hive + Redis)", use_container_width=True)

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
                st.dataframe(df_batch, use_container_width=True, height=360)
            except Exception as e:
                st.warning(f"Batch (Hive) no listo todavia (OK): {e}")

        with right:
            st.subheader("Speed (Redis)")
            try:
                df_rt2 = cached_realtime_stats(product_filter)
                st.success(f"Speed OK: {len(df_rt2)} filas (snapshot)")
                st.dataframe(df_rt2, use_container_width=True, height=360)
            except Exception as e:
                st.warning(f"Speed (Redis) no listo todavia (OK): {e}")

        st.divider()

        # Normalizamos para comparar por event_name
        if not df_batch.empty and ("event_name" in df_batch.columns) and (("cnt" in df_batch.columns) or ("count" in df_batch.columns)):
            batch_col = "cnt" if "cnt" in df_batch.columns else "count"
            batch_by_event = (
                df_batch.groupby("event_name", as_index=False)[batch_col]
                .sum()
                .rename(columns={batch_col: "batch_cnt"})
            )
        else:
            batch_by_event = pd.DataFrame(columns=["event_name", "batch_cnt"])

        if not df_rt2.empty and ("event_name" in df_rt2.columns) and ("count" in df_rt2.columns):
            rt_by_event = (
                df_rt2.groupby("event_name", as_index=False)["count"]
                .sum()
                .rename(columns={"count": "realtime_cnt"})
            )
        else:
            rt_by_event = pd.DataFrame(columns=["event_name", "realtime_cnt"])

        merged = pd.merge(batch_by_event, rt_by_event, on="event_name", how="outer").fillna(0)
        if merged.empty:
            st.info("No hay suficiente informacion para comparar (falta Batch o Speed).")
        else:
            merged["batch_cnt"] = _safe_int_series(merged["batch_cnt"])
            merged["realtime_cnt"] = _safe_int_series(merged["realtime_cnt"])
            merged["delta_speed_minus_batch"] = merged["realtime_cnt"] - merged["batch_cnt"]

            cA, cB, cC = st.columns(3)
            cA.metric("Eventos distintos (merge)", _fmt_int(merged["event_name"].nunique()))
            cB.metric("Suma Batch", _fmt_int(merged["batch_cnt"].sum()))
            cC.metric("Suma Speed", _fmt_int(merged["realtime_cnt"].sum()))

            grouped_compare_chart(
                merged,
                category="event_name",
                a="batch_cnt",
                b="realtime_cnt",
                title="Comparacion por evento (Top)",
                top_n=top_n,
                height=520,
            )

            st.subheader("Tabla comparativa")
            st.dataframe(
                merged.sort_values(["batch_cnt", "realtime_cnt"], ascending=False),
                use_container_width=True,
                height=460,
            )