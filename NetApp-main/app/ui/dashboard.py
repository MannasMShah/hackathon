import time, requests, pandas as pd, streamlit as st

API = "http://api:8000"

st.set_page_config(page_title="NetApp Data-in-Motion", layout="wide")
st.title("📊 NetApp Data-in-Motion — Unified Dashboard")

colA, colB = st.columns(2)

with colA:
    st.subheader("Files (metadata + location)")
    try:
        files = requests.get(f"{API}/files", timeout=3).json()
        df = pd.DataFrame(files)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"API not reachable: {e}")

with colB:
    st.subheader("Policy Recommendation")
    file_id = st.text_input("Enter file id", "file_001.txt")
    if st.button("Get Policy"):
        try:
            res = requests.get(f"{API}/policy/{file_id}", timeout=3).json()
            st.write(res)
        except Exception as e:
            st.error(e)

st.subheader("Actions")
c1, c2, c3 = st.columns(3)
with c1:
    if st.button("Simulate Access Event"):
        try:
            r = requests.post(f"{API}/ingest_event", json={"file_id": file_id})
            st.success(r.json())
        except Exception as e:
            st.error(e)

with c2:
    target = st.selectbox("Move target", ["s3","azure","gcs"])
with c3:
    if st.button("Move File"):
        try:
            r = requests.post(f"{API}/move", json={"file_id": file_id, "target": target}).json()
            st.success(r)
        except Exception as e:
            st.error(e)

st.caption("Refresh the page to see updated locations.")
