import streamlit as st

from langchain_huggingface import HuggingFaceEmbeddings
from langchain_postgres.vectorstores import PGVector
from utils.db_utils import get_db_params
from utils.st_utils import display_success_message

@st.cache_resource
def load_embeddings():
    return HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

@st.cache_resource
def load_vector_store(_embeddings): # add an underscore to tell streamlit not to has the argument
    db_params = get_db_params()
    schema = 'vector_store,public'
    connection_string = (
        f"postgresql+psycopg://{db_params['user']}:"
        f"{db_params['password']}@"
        f"{db_params['host']}:"
        f"{db_params['port']}/"
        f"{db_params['database']}"
        f"?options=-csearch_path%3D{schema}"
    )
    collection_name = "game_embeddings"
    distance_strategy = 'cosine'

    vector_store = PGVector(
        embeddings=embeddings,
        collection_name=collection_name,
        connection=connection_string,
        use_jsonb=True,
        distance_strategy=distance_strategy
    )
    
    return vector_store

embeddings = load_embeddings()
vector_store = load_vector_store(embeddings)

query = st.text_input("🔍 Describe the game you're looking for:")

if query:
    results = vector_store.similarity_search_with_score(query, k=5)
    if results:
        display_success_message("✅ Query executed successfully!")
    st.markdown("## 🔎 Top Recommendations")
    for i, (doc, score) in enumerate(results):
        game_name = doc.metadata.get('name', 'Unknown Game')
        content_preview = doc.page_content[:300]
        
        st.markdown(f"### {i+1}. {game_name}")
        st.markdown(f"**Similarity Score**: `{score:.4f}`")
        st.write(content_preview)
        st.markdown("---")

with st.form("my_form"):
    query = st.text_area(
        "Enter text:",
        "Games with Cowboys!",
    )
    submitted = st.form_submit_button("Submit")
    if submitted:
        results = vector_store.similarity_search_with_score(query, k=5)
        if results:
            display_success_message("✅ Query executed successfully!")
        st.markdown("## 🔎 Top Recommendations")
        for i, (doc, score) in enumerate(results):
            game_name = doc.metadata.get('name', 'Unknown Game')
            content_preview = doc.page_content[:300]
            
            st.markdown(f"### {i+1}. {game_name}")
            st.markdown(f"**Similarity Score**: `{score:.4f}`")
            st.write(content_preview)
            st.markdown("---")