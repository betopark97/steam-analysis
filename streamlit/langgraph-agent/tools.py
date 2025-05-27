from langchain.tools import tool
from langchain_tavily import TavilySearch
from langchain_community.tools import DuckDuckGoSearchResults

@tool
def tavily_search(query):
    """Returns search results from Tavily."""
    return TavilySearch(max_results=2).invoke(query)

@tool
def duckduckgo_search(query):
    """Returns search results from DuckDuckGo."""
    return DuckDuckGoSearchResults(output_format='json').invoke(query)