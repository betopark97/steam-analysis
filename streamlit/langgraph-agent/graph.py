from state import State
from nodes import (
    decide_tool,
    search_database_node,
    search_web_node,
    answer_question_node
)
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import InMemorySaver


# Instantiate workflow
workflow = StateGraph(State)
workflow.add_node("Decide Tool", decide_tool)
workflow.add_node("Search DB", search_database_node)
workflow.add_node("Search Web", search_web_node)
workflow.add_node("Answer Question", answer_question_node)

workflow.add_edge(START, "Decide Tool")
workflow.add_conditional_edges(
    "Decide Tool", 
    lambda state: state["next"], 
    # decide_tool, 
    {
        "Search DB": "Search DB",
        "Search Web": "Search Web",
        "Answer Question": "Answer Question"
    }
)
workflow.add_edge("Search DB", END)
workflow.add_edge("Search Web", END)
workflow.add_edge("Answer Question", END)


checkpointer = InMemorySaver()
app = workflow.compile(checkpointer=checkpointer)

initial_state = {
    # "question": "What is the most popular game on Steam?",
    # "question": "What is the weather in South Korea right now?",
    "question": "What are some popular genres on Steam Games?",
}
config = {"configurable": {"thread_id": "1"}}
result = app.invoke(initial_state, config)
print(result['answer'])



# LangGraph Visualization
from IPython.display import Image, display
from langchain_core.runnables.graph_mermaid import MermaidDrawMethod

# Visualize
Image(
    app
    .get_graph()
    .draw_mermaid_png(
        max_retries=5, retry_delay=5, draw_method=MermaidDrawMethod.API,
        output_file_path='../../reports/figures/langgraph_viz.png'
    )
)









