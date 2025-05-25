import streamlit as st
import time


def display_success_message(message):
    message_container = st.empty()
    message_container.success(message)
    time.sleep(1)
    message_container.empty()