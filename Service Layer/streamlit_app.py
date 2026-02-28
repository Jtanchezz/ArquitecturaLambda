from datetime import datetime, timedelta, timezone
import streamlit as st
import pandas as pd

# Import robusto (evita el "cannot import name SETTINGS")
try:
    import serving

    # Si por alguna razón SETTINGS no existe, lo creamos (deja la app "lista")
    if not hasattr(serving, "SETTINGS") and hasattr(serving, "Settings"):
        serving.SETTINGS = serving.Settings()

    if not hasattr(serving, "SETTINGS"):
        raise RuntimeError("serving.py no define SETTINGS (ni se pudo reconstruir).")

except Exception as e:
    st.set_page_config(page_title="Serving Layer - Lambda", layout="wide")
    st.title("Serving Layer (Lambda Architecture)")
    st.error(f"No pude cargar serving.py: {e}")
    st.stop()

SETTINGS = serving.SETTINGS

st.set_page_config(page_title="Serving Layer - Lambda", layout="wide")
st.title("Serving Layer (Lambda Architecture)")
st.caption("Speed (Redis) + Batch (Hive) + Serving (Streamlit)")

# ---------- Sidebar ----------
st.sidebar.header("Conexiones (desde .env)")
st.sidebar.write("Redis:", f"{SETTINGS.redis_host}:{SETTINGS.redis_port}", f"db={SETTINGS.redis_db}")
st.sidebar.write("Hive:", f"{SETTINGS.hive_host}:{SETTINGS.hive_port}", f"db={SETTINGS.hive_database}")
st.sidebar.write("Tabla Hive:", SETTINGS.hive_events_table)
st.sidebar.divider()

product_filter = st.sidebar.text_input("Filtrar por product_id (opcional)", value="").strip() or None

now = datetime.now(timezone.utc).replace(microsecond=0)
default_start = (now - timedelta(hours=24)).replace(microsecond=0)

start = st.sidebar.date_input("Inicio (UTC)", value=default_start.date())
end = st.sidebar.date_input("Fin (UTC)", value=now.date())

start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
end_dt = datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)

start_iso = start_dt.isoformat().replace("+00:00", "Z")
end_iso = end_dt.isoformat().replace("+00:00", "Z")

tab_rt, tab_hist, tab_lambda = st.tabs(["⚡ Realtime (Redis)", "📚 Histórico (Hive)", "🧠 Lambda View (mixto)"])

# ---------- Realtime ----------
with tab_rt:
    st.subheader("Realtime (Speed Layer) desde Redis")
    try:
        df_rt = serving.get_realtime_stats(product_id=product_filter)

        c1, c2 = st.columns([2, 1])

        with c1:
            st.write("Eventos por producto y tipo (snapshot):")
            st.dataframe(df_rt, use_container_width=True, height=420)

        with c2:
            st.write("Top eventos (sumado):")
            if df_rt.empty:
                st.info("No hay datos en Redis todavía (esto es OK si aún no corre el streaming).")
            else:
                top_events = (
                    df_rt.groupby("event_name", as_index=False)["count"]
                    .sum()
                    .sort_values("count", ascending=False)
                )
                st.dataframe(top_events, use_container_width=True, height=250)

                st.write("Top productos (sumado):")
                top_products = (
                    df_rt.groupby("product_id", as_index=False)["count"]
                    .sum()
                    .sort_values("count", ascending=False)
                    .head(15)
                )
                st.dataframe(top_products, use_container_width=True, height=250)

    except Exception as e:
        st.warning(f"Redis no disponible o sin datos: {e}")

# ---------- Histórico ----------
with tab_hist:
    st.subheader("Histórico (Batch Layer) desde Hive")
    st.write("Rango:", start_iso, "→", end_iso)

    sql_default = serving.default_hive_sql(start_iso, end_iso)
    sql = st.text_area("SQL en Hive (puedes ajustarlo)", value=sql_default, height=160)

    run = st.button("▶️ Ejecutar SQL en Hive")
    if run:
        try:
            df = serving.hive_query(sql)
            st.success(f"OK: {len(df)} filas")
            st.dataframe(df, use_container_width=True, height=420)
        except Exception as e:
            st.warning(f"Hive no disponible / tabla no existe todavía (OK por ahora): {e}")

# ---------- Lambda View ----------
with tab_lambda:
    st.subheader("Lambda View (Batch + Speed)")
    try:
        wm = serving.get_batch_watermark()
    except Exception as e:
        wm = None
        st.warning(f"No pude leer watermark: {e}")

    if wm:
        st.write("✅ Watermark:", wm.isoformat().replace("+00:00", "Z"))
    else:
        st.write("⚠️ Watermark no encontrado (OK por ahora). Key esperada:", f"`{SETTINGS.watermark_key}`")

    st.info(serving.lambda_view_note())

    st.markdown("### Ejecutar batch (Hive) + snapshot (Redis)")
    sql_batch = serving.default_hive_sql(start_iso, end_iso)
    sql_batch_user = st.text_area("SQL batch (Hive)", value=sql_batch, height=140)

    run_mix = st.button("▶️ Ejecutar (Hive) + Snapshot (Redis)")
    if run_mix:
        try:
            df_batch = serving.hive_query(sql_batch_user)
            st.success(f"Batch OK: {len(df_batch)} filas")
            st.dataframe(df_batch, use_container_width=True, height=280)
        except Exception as e:
            st.warning(f"Batch (Hive) no listo todavía (OK): {e}")

        try:
            df_rt = serving.get_realtime_stats(product_id=product_filter)
            st.dataframe(df_rt, use_container_width=True, height=280)
        except Exception as e:
            st.warning(f"Speed (Redis) no listo todavía (OK): {e}")