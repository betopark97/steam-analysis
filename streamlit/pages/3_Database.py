import os
import time
import pandas as pd
import polars as pl
from sqlalchemy import create_engine
import streamlit as st
import streamlit_mermaid as stmd
from utils.st_utils import display_success_message
from utils.db_utils import get_db_params


db_params = get_db_params()

# --- Build connection string ---
connection_string = (
    f"postgresql+psycopg://{db_params['user']}:"
    f"{db_params['password']}@"
    f"{db_params['host']}:"
    f"{db_params['port']}/"
    f"{db_params['database']}"
)

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# --- Function to get table names with schema ---
def get_schema_table_dict(connection):
    query = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT ILIKE 'pg_%%'
      AND table_schema != 'information_schema';
    """
    df = pd.read_sql(query, connection)
    schema_table_dict = (
        df.groupby("table_schema")["table_name"]
        .apply(list)
        .to_dict()
    )

    return schema_table_dict


# --- Connection and table retrieval button ---
if st.button("🔌 Connect and Load Schemas"):
    try:
        with engine.connect() as conn:
            schema_dict = get_schema_table_dict(conn)
            st.session_state['schema_dict'] = schema_dict
            display_success_message(f"🎉 Found {len(schema_dict)} schemas.")
    except Exception as e:
        st.error(f"Connection failed: {e}")

# --- Dropdown and preview ---
if 'schema_dict' in st.session_state:

    # --- Generate Mermaid Diagram Code ---
    graph_lines = ["graph TD"]
    for schema, tables in st.session_state['schema_dict'].items():
        schema_node = schema.replace("-", "_")  # sanitize if needed
        graph_lines.append(f'{schema_node}["📂 {schema}"]')
        for table in tables:
            table_node = f"{schema_node}_{table}".replace("-", "_")
            graph_lines.append(f'{schema_node} --> {table_node}["🗂️ {table}"]')
    code = "\n".join(graph_lines)
    stmd.st_mermaid(code)

    col1, col2 = st.columns(2)
    with col1:
        selected_schema = st.selectbox("📁 Select a Schema", st.session_state['schema_dict'].keys())
    with col2:
        selected_table = st.selectbox("📁 Select a Table", st.session_state['schema_dict'][selected_schema])
        
    # --- Table Preview ---
    if selected_schema and selected_table:
        preview_query = f'SELECT * FROM "{selected_schema}"."{selected_table}" LIMIT 10'
        try:
            with engine.connect() as conn:
                df_preview = pd.read_sql(preview_query, conn)
                st.markdown(f"### Preview of `{selected_schema}.{selected_table}`")
                with st.expander("Click to View"):
                    st.dataframe(df_preview)
        except Exception as e:
            st.error(f"❌ Failed to load preview: {e}")

# UI
st.title("📊 PostgreSQL Data Viewer")
query = st.text_input("Enter SQL Query", "SELECT * FROM your_table LIMIT 10")

if st.button("Run Query"):
    with engine.connect() as conn:
        try:
            df = pd.read_sql(query, conn)
            display_success_message("✅ Query executed successfully!")
            st.dataframe(df)
            # with st.expander("Preview"):
            #     st.dataframe(df)
        except Exception as e:
            st.error(f"❌ Failed to execute query: {e}")
            
            
            