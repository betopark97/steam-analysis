from langgraph.graph.message import add_messages
from typing import TypedDict, Annotated, List
from langchain_core.documents import Document
from graph import State
from langgraph.graph import StateGraph, START, END
from nodes import (
    evaluate_prompt, get_database_info,
    answer_question, search_the_web,
    query_database, validate_result
)


class State(TypedDict):
    context: Annotated[List[Document], add_messages]
    answer: Annotated[List[Document], add_messages]
    question: Annotated[str, 'user question']
    sql_query: Annotated[str, 'sql query']
    binary_score: Annotated[str, 'binary score yes or no']
    
workflow = StateGraph(State)

workflow.add_node("Execute Gemini", evaluate_prompt)
workflow.add_conditional_edges(

)
workflow.add_node("")