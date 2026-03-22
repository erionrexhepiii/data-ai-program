import streamlit as st
import pandas as pd
from datetime import datetime

import config
import claude_client
import databricks_client
from utils import is_dangerous, dangerous_keyword

st.set_page_config(
    page_title="Databricks AI Assistant",
    page_icon="⚡",
    layout="wide",
)

config.init_session_state()

MAX_HISTORY = 10

# sidebar
with st.sidebar:
    st.title("⚡ Databricks Assistant")
    st.caption("Powered by Claude")

    with st.expander("🔧 Configuration", expanded=not config.has_sql_config()):
        config.render_sidebar_fields()

    col_test, col_schema = st.columns(2)

    with col_test:
        if st.button("🔌 Test Connection", use_container_width=True):
            if not config.has_sql_config():
                st.error("Fill in Workspace URL, Token, and Warehouse ID first.")
            else:
                with st.spinner("Testing..."):
                    try:
                        info = databricks_client.test_connection(
                            config.get("workspace_url"),
                            config.get("token"),
                            config.get("warehouse_id"),
                        )
                        state_note = ""
                        if info["state"] and info["state"] != "RUNNING":
                            state_note = f" (state: {info['state']})"
                        st.success(f"Connected — {info['name']}{state_note}")
                    except Exception as e:
                        st.error(f"Connection failed: {e}")

    with col_schema:
        if st.button("📂 Load Schema", use_container_width=True):
            if not (config.get("workspace_url") and config.get("token")):
                st.error("Fill in Workspace URL and Token first.")
            else:
                with st.spinner("Loading schema..."):
                    try:
                        tables = databricks_client.load_schema(
                            config.get("workspace_url"),
                            config.get("token"),
                        )
                        st.session_state["schema"] = tables
                        st.success(f"Schema loaded — {len(tables)} table{'s' if len(tables) != 1 else ''} found")
                    except Exception as e:
                        st.error(f"Schema load failed: {e}")

    if st.session_state.get("schema"):
        schema = st.session_state["schema"]
        with st.expander(f"📋 Loaded Schema ({len(schema)} tables)"):
            for tbl in schema:
                st.markdown(f"**{tbl['full_name']}**")
                col_lines = [f"- `{c['name']}` *{c['type_name']}*" for c in tbl.get("columns", [])]
                if col_lines:
                    st.markdown("\n".join(col_lines))
                else:
                    st.caption("No columns found")

# main panel
st.markdown("## 💬 Ask your database anything")

mode = st.radio(
    "Execution mode",
    options=["SQL", "PySpark"],
    horizontal=True,
    index=0 if st.session_state.get("mode", "SQL") == "SQL" else 1,
    key="mode_radio",
)
st.session_state["mode"] = mode
mode_label = mode

user_prompt = st.text_area(
    "Describe what you need in plain English",
    placeholder='Examples: "Show me the top 10 customers by revenue", "Count orders placed last month"',
    height=100,
    key="user_prompt",
)

if st.button(f"🚀 Generate {mode_label}", type="primary", disabled=not user_prompt.strip()):
    if not config.has_claude_config():
        st.error("Please set your Claude API key in the sidebar.")
    else:
        with st.spinner(f"Claude is generating {mode_label}..."):
            try:
                code = claude_client.generate_code(
                    api_key=config.get("claude_api_key"),
                    user_message=user_prompt.strip(),
                    mode=mode,
                    schema=st.session_state.get("schema"),
                )
                st.session_state["generated_code"] = code
            except Exception as e:
                st.error(f"Claude API error: {e}")
                st.session_state["generated_code"] = ""

# code preview and execution
if st.session_state.get("generated_code"):
    st.markdown(f"### 📝 Generated {mode_label}")
    st.caption("You can edit the code below before executing.")

    edited_code = st.text_area(
        "Code preview",
        value=st.session_state["generated_code"],
        height=200,
        key="code_editor",
        label_visibility="collapsed",
    )
    st.session_state["generated_code"] = edited_code

    dangerous = is_dangerous(edited_code, mode.lower())
    confirmed = True

    if dangerous:
        kw = dangerous_keyword(edited_code, mode.lower())
        st.warning(
            f"⚠️ This query contains **{kw}** and may modify or delete data. "
            "Please confirm you want to proceed."
        )
        confirmed = st.checkbox(
            "I understand this is a destructive operation and want to execute it",
            key="danger_confirm",
        )

    col_exec, col_clear = st.columns([1, 1])

    with col_exec:
        execute_disabled = (dangerous and not confirmed) or not edited_code.strip()
        if st.button("▶️ Execute", type="primary", disabled=execute_disabled, use_container_width=True):
            if mode == "SQL" and not config.has_sql_config():
                st.error("Please configure Databricks Workspace URL, Token, and Warehouse ID.")
            elif mode == "PySpark" and not config.has_pyspark_config():
                st.error("PySpark mode requires Workspace URL, Token, and Cluster ID.")
            else:
                with st.spinner(f"Executing {mode_label} on Databricks..."):
                    try:
                        if mode == "SQL":
                            result = databricks_client.execute_sql(
                                config.get("workspace_url"),
                                config.get("token"),
                                config.get("warehouse_id"),
                                edited_code,
                            )
                        else:
                            result = databricks_client.execute_pyspark(
                                config.get("workspace_url"),
                                config.get("token"),
                                config.get("cluster_id"),
                                edited_code,
                            )

                        row_count = result["row_count"]
                        st.success(f"Query executed successfully — {row_count} row{'s' if row_count != 1 else ''} returned")

                        if result["columns"]:
                            df = pd.DataFrame(result["rows"], columns=result["columns"])
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.info("Query executed successfully. No rows returned.")

                        entry = {
                            "prompt": user_prompt.strip(),
                            "code": edited_code,
                            "mode": mode,
                            "row_count": row_count,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "error": None,
                        }
                        history = st.session_state.get("query_history", [])
                        st.session_state["query_history"] = [entry] + history[:MAX_HISTORY - 1]

                    except Exception as e:
                        st.error(f"Execution error: {e}")

                        entry = {
                            "prompt": user_prompt.strip(),
                            "code": edited_code,
                            "mode": mode,
                            "row_count": None,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "error": str(e),
                        }
                        history = st.session_state.get("query_history", [])
                        st.session_state["query_history"] = [entry] + history[:MAX_HISTORY - 1]

    with col_clear:
        if st.button("🗑️ Clear", use_container_width=True):
            st.session_state["generated_code"] = ""
            st.rerun()

# query history
history = st.session_state.get("query_history", [])
if history:
    with st.expander(f"📜 Query History (last {len(history)})", expanded=False):
        for i, h in enumerate(history):
            status_icon = "✅" if h.get("error") is None else "❌"
            row_info = f"{h['row_count']} rows" if h.get("row_count") is not None else "Error"
            col_hist_text, col_hist_btn = st.columns([4, 1])
            with col_hist_text:
                st.markdown(
                    f"{status_icon} **{h['prompt'][:80]}{'...' if len(h['prompt']) > 80 else ''}**  \n"
                    f"<small>{h['mode']} · {h['timestamp']} · {row_info}</small>",
                    unsafe_allow_html=True,
                )
            with col_hist_btn:
                if st.button("Reload", key=f"hist_{i}", use_container_width=True):
                    st.session_state["generated_code"] = h["code"]
                    st.session_state["mode"] = h["mode"]
                    st.rerun()
