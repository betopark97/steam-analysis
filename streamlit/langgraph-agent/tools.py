from langchain.agents import create_sql_agent, AgentType
from langchain_community.utilities import SQLDatabase
from langchain.tools import tool
from models import Model
import os
from dotenv import load_dotenv
from langchain_tavily import TavilySearch
from langchain_community.tools import DuckDuckGoSearchResults


load_dotenv()

model = Model()
db = SQLDatabase.from_uri(
    f"postgresql://{os.environ.get('DB_USER')}:"
    f"{os.environ.get('DB_PASSWORD')}@"
    f"{os.environ.get('DB_HOST')}:"
    f"{os.environ.get('DB_PORT')}/"
    f"{os.environ.get('DB_NAME')}"
    f"?options=-csearch_path%3Dsilver"
)

sql_agent = create_sql_agent(
    llm=model.chat_model_gemini, 
    db=db, 
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True
)

@tool
def search_db(query: str):
    """Use natural language to query the PostgreSQL database."""
    return sql_agent.invoke(query)

@tool
def search_web(query: str):
    """Use natural language to query the web."""
    return DuckDuckGoSearchResults(output_format='json').invoke(query)

@tool
def tavily_search(query: str):
    """Returns search results from Tavily."""
    return TavilySearch(max_results=2).invoke(query)

@tool
def duckduckgo_search(query: str):
    """Returns search results from DuckDuckGo."""
    return DuckDuckGoSearchResults(output_format='json').invoke(query)