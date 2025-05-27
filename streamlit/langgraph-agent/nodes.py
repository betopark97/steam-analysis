from graph import State
from langgraph.prebuilt import ToolNode
from tools import duckduckgo_search, tavily_search


tools = [tavily_search, duckduckgo_search]
tool_node = ToolNode(tools)


def evaluate_prompt(state: State): # execute gemini
    pass

def get_database_info(state: State):
    pass

def query_database(state: State):
    pass

def validate_result(state: State):
    pass

def search_the_web(state: State):
    pass

def answer_question(state: State):
    pass