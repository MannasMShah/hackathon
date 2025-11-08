import os, time, requests, pandas as pd, streamlit as st
st.set_page_config(page_title='NetApp Data-in-Motion', layout='wide')
st.title('📡 NetApp Data in Motion — Live Stream')

API_HOST = os.getenv('STREAM_API_HOST', 'localhost')
API_PORT = int(os.getenv('STREAM_API_PORT', '8001'))
BASE = f"http://{API_HOST}:{API_PORT}"

def fetch_json(path):
    r = requests.get(f"{BASE}{path}", timeout=2)
    r.raise_for_status()
    return r.json()

def to_df(x):
    # [] -> empty DF; dict -> [dict]; scalar -> {"value":[x]}
    if x is None:
        return pd.DataFrame()
    if isinstance(x, list):
        return pd.DataFrame(x) if x else pd.DataFrame()
    if isinstance(x, dict):
        return pd.DataFrame([x])
    return pd.DataFrame({"value":[x]})

col1, col2, col3 = st.columns(3)
with col1:
    if st.button('Ping /health'):
        try:
            st.json(fetch_json('/health'))
        except Exception as e:
            st.error(f'Health failed: {e}')

with col2:
    top_n = st.number_input('Peek N events', min_value=5, max_value=200, value=25, step=5)

with col3:
    refresh = st.checkbox('Auto-refresh', value=True)

ph_health = st.empty()
ph_actions = st.empty()
ph_events = st.empty()

while True:
    try:
        health  = fetch_json('/health')
        actions = fetch_json('/actions?n=25')
        events  = fetch_json(f'/stream/peek?n={int(top_n)}')

        with ph_health.container():
            st.subheader('Health')
            st.json(health)

        with ph_actions.container():
            st.subheader('Recent Actions')
            st.dataframe(to_df(actions), use_container_width=True)

        with ph_events.container():
            st.subheader('Recent Events')
            df = to_df(events)
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                if {'timestamp','temperature'}.issubset(df.columns):
                    plot_df = df[['timestamp','temperature']].sort_values('timestamp')
                    st.line_chart(plot_df.set_index('timestamp'))
            else:
                st.info('Waiting for events…')
    except Exception as e:
        st.error(f'Failed to fetch: {e}')

    if not refresh:
        break
    time.sleep(1.2)
