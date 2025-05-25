import streamlit as st


st.set_page_config(
    page_title="Steam Analysis",
    page_icon="./img/steam_logo.svg",
    layout="wide",
    initial_sidebar_state="expanded",
)

#################################################################################
# Sidebar
#################################################################################
with st.sidebar:
    st.title("Navigation")
    st.subheader("Pages")
    
#################################################################################
# Header
#################################################################################
steam_logo = st.image('./img/steam_logo.svg', width=300)
st.title("Steam Game Analysis!")

#################################################################################
# Body
#################################################################################
st.markdown("""
Hello, I'm Roberto!

I'm currently a Data Analyst at The Pinkfong Company.

This is a Data Engineering project that I made to practice a set of different tools.

I hope you enjoy it!   

Keep in mind that this project was created to guide indie developers and help them find their audience.
         
""")
