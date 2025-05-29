from typing import TypedDict, Annotated, List
from langchain_core.documents import Document
from langgraph.graph.message import add_messages


class State(TypedDict):
    question: Annotated[str, 'user question']
    answer: Annotated[List[Document], add_messages]
    sql_query: Annotated[str, 'SQL query for DB']
    web_results: Annotated[str, 'Search results from the web']
    context: Annotated[List[Document], add_messages]
    binary_score: Annotated[str, 'binary score yes or no']