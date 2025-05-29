from langchain_core.prompts import ChatPromptTemplate

prompt_template = ChatPromptTemplate([
    ("system", "You are a helpful assistant"),
    ("user", "Tell me a joke about {topic}")
])

prompt_template.invoke({"topic": "cats"})

Web Search Tool Prompt

“Use the web search tool if the question refers to current events, news, or real-time information. Return summarized search results in a concise format.”

DB Search Tool Prompt

“Use the database tool if the question asks about game details, stats, or metrics stored in the Steam database.”

Direct Answer Prompt

“Answer directly if the question can be answered without external tools or knowledge.”