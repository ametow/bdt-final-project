"""Streamlit dashboard backed by HBase (Thrift)."""

import os
import time
from datetime import datetime

import happybase
import pandas as pd
import streamlit as st

HBASE_HOST = os.environ.get("HBASE_HOST", "hbase")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))

st.set_page_config(page_title="BDT Crypto Pipeline", layout="wide")
st.title("Real-time Crypto Trades — BDT Final Project")
st.caption("Binance WS → Kafka → Spark → HBase → Streamlit")


def get_connection():
    return happybase.Connection(HBASE_HOST, port=HBASE_PORT, autoconnect=True)


def decode_row(row: dict) -> dict:
    return {k.decode().split(":", 1)[-1]: v.decode() for k, v in row.items()}


def fetch_latest():
    conn = get_connection()
    try:
        table = conn.table("trade_latest")
        out = []
        for key, row in table.scan():
            d = decode_row(row)
            d["symbol"] = key.decode()
            out.append(d)
    finally:
        conn.close()
    return pd.DataFrame(out)


def fetch_agg(limit_per_symbol: int = 60):
    conn = get_connection()
    try:
        table = conn.table("trade_agg")
        out = []
        for key, row in table.scan():
            d = decode_row(row)
            d["rowkey"] = key.decode()
            out.append(d)
    finally:
        conn.close()
    if not out:
        return pd.DataFrame()
    df = pd.DataFrame(out)
    for col in ["avg_price", "vwap", "min_price", "max_price", "volume", "count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "window_end" in df.columns:
        df["window_end"] = pd.to_datetime(
            pd.to_numeric(df["window_end"], errors="coerce"), unit="ms"
        )
    df = df.sort_values("window_end")
    df = df.groupby("symbol", group_keys=False).tail(limit_per_symbol)
    return df


refresh = st.sidebar.slider("Auto-refresh (s)", 2, 30, 5)
window_n = st.sidebar.slider("Windows shown per symbol", 10, 240, 60)

placeholder = st.empty()

while True:
    try:
        latest = fetch_latest()
        agg = fetch_agg(window_n)
    except Exception as exc:
        with placeholder.container():
            st.error(f"HBase connection error: {exc}")
        time.sleep(refresh)
        continue

    with placeholder.container():
        st.subheader("Live ticker")
        if latest.empty:
            st.info("Waiting for first records…")
        else:
            cols = st.columns(min(len(latest), 4) or 1)
            for col, (_, row) in zip(cols, latest.iterrows()):
                col.metric(
                    label=f"{row['symbol']} · {row.get('name','')}",
                    value=f"{float(row['price']):,.4f}",
                    delta=f"qty {float(row['qty']):.4f}",
                )

        st.subheader("Windowed metrics (1-min)")
        if agg.empty:
            st.info("No aggregated windows yet.")
        else:
            for sym, g in agg.groupby("symbol"):
                st.markdown(f"**{sym}**")
                chart_df = g.set_index("window_end")[["vwap", "avg_price"]]
                st.line_chart(chart_df, height=180)
                vol_df = g.set_index("window_end")[["volume"]]
                st.bar_chart(vol_df, height=120)

            anomalies = agg[agg["anomaly"].astype(str).str.lower() == "true"]
            st.subheader("Anomalies")
            if anomalies.empty:
                st.success("No anomalies in current window.")
            else:
                st.dataframe(
                    anomalies[["window_end", "symbol", "avg_price", "vwap", "volume", "count"]]
                    .sort_values("window_end", ascending=False),
                    use_container_width=True,
                )

        st.caption(f"Last refresh: {datetime.utcnow().isoformat(timespec='seconds')}Z")

    time.sleep(refresh)
    st.rerun()
