from state import State
from tools import search_db, search_web
from models import Model
from langgraph.prebuilt import ToolNode

model = Model()
chat_agent = model.chat_model_gemini

tools = [search_db, search_web]
# react_agent = create_react_agent(
#     model=chat_agent,
#     tools=tools
# )
chat_agent_with_tools = chat_agent.bind_tools(tools)
tool_node = ToolNode(tools)

def decide_tool(state: State) -> State:
    result = chat_agent_with_tools.invoke(state["question"])
    
    if "search_db" in str(result):
        return {"next": "Search DB"}
    elif "search_web" in str(result):
        return {"next": "Search Web"}
    else:
        return {"next": "Answer Question"}
    
def search_database_node(state: State) -> State:
    print("Question passed to search_db:", state["question"]) 
    result = search_db.invoke(state["question"])
    return State(sql_query=result['output'], answer=result['output'])

def search_web_node(state: State) -> State:
    result = search_web.invoke(state["question"])
    return State(web_results=result, answer=result)

def answer_question_node(state: State) -> State:
    result = chat_agent.invoke({"input": state["question"]})
    return State(answer=result)