import os
from dotenv import load_dotenv
from langchain_google_genai.chat_models import ChatGoogleGenerativeAI
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_huggingface import HuggingFaceEmbeddings


load_dotenv()


class Model:
    def __init__(self):
        self.chat_model_gemini = ChatGoogleGenerativeAI(
            api_key=os.environ.get("GEMINI_API_KEY"),
            model='gemini-2.5-flash-preview-04-17',
            temperature=0.6
        )
        self.embedding_model_gemini = GoogleGenerativeAIEmbeddings(
            google_api_key=os.environ.get("GEMINI_API_KEY"),
            model="models/gemini-embedding-exp-03-07"
        )
        self.embedding_model_huggingface = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")