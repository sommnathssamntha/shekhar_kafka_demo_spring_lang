import os
import yaml
from typing import TypedDict, List, Optional, Annotated
from operator import add
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph, END

load_dotenv()

# 1. DEFINE THE STATE (The 'Brain' memory)
class KafkaState(TypedDict):
    question: str
    infrastructure_report: Optional[str]
    messages: Annotated[List[BaseMessage], add]
    final_answer: Optional[str]

class KafkaNodes:
    def __init__(self, llm, config_path="config.yaml"):
        self.llm = llm
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self._agent = None

    # --- THE TOOLS (The 'Hands') ---
    def _build_tools(self):
        from langchain_core.tools import Tool
        import subprocess

        def kafka_check_fn(query: str) -> str:
            """Tool to run the Windows .bat file for Kafka Lag"""
            cmd = [
                self.config['kafka']['bin_path'], 
                "--bootstrap-server", self.config['kafka']['bootstrap_server'],
                "--describe", "--group", self.config['kafka']['group_id']
            ]
            try:
                res = subprocess.run(cmd, capture_output=True, text=True, shell=True)
                return res.stdout if res.stdout else "Kafka is idle."
            except Exception as e:
                return f"CLI Error: {str(e)}"

        def log_reader_fn(query: str) -> str:
            """Forensic tools: Dumps binary kafka logs when App logs are missing"""

            # Point to the 'kafka-run-class.bat' in your bin/windows folder
            kafka_run_class = self.config['kafka']['bin_path'].replace("kafka-consumer-groups.bat", "kafka-run-class.bat")
           
            # The path to your binary .log file 
            log_file = self.config['java_app']['log_path']
            cmd = [
                kafka_run_class, "kafka.tools.DumpLogSegments",
                "--deep-iteration", "--print-data-log", "--files", log_file
            ]
            try:
                # use subprocess to run the command and capture output
                res= subprocess.run(cmd, capture_output=True, text=True, shell=True)
                return res.stdout if res.stdout else "Binary log is empty."
            except Exception as e:
                return f"Audit Read Error: {str(e)}"

        return [
            Tool(name="kafka_monitor", func=kafka_check_fn, description="Check partition lag/status"),
            Tool(name="log_analyzer", func=log_reader_fn, description="Read Java app logs for exceptions")
        ]

    # --- THE AGENT (The 'Reasoning Engine') ---
    def _build_agent(self):
        tools = self._build_tools()
        system_prompt = (
            "You are a Senior Kafka SRE Agent."
            "First, use 'kafka_monitor' to check for lag."
            "If lag is > 0, use 'log_analyzer' to find the root cause (e.g. DB errors or Serialisation issues)."
        )
        return create_react_agent(self.llm, tools=tools, prompt=system_prompt)

    # --- THE NODES (The 'Steps') ---
    def analyze_infrastructure(self, state: KafkaState):
        """Node 1: Checks Kafka and updates state"""
        if self._agent is None:
            self._agent = self._build_agent()
        
        # Invoke the ReAct agent inside the node
        inputs = {"messages": [HumanMessage(content=state['question'])]}
        result = self._agent.invoke(inputs)
        
        return {
            "messages": result["messages"],
            "final_answer": result["messages"][-1].content
        }

# 3. BUILD THE GRAPH (The 'Workflow')
workflow = StateGraph(KafkaState)
llm = ChatOpenAI(model="gpt-4o", temperature=0)
nodes = KafkaNodes(llm)

workflow.add_node("diagnose", nodes.analyze_infrastructure)
workflow.set_entry_point("diagnose")
workflow.add_edge("diagnose", END)

app = workflow.compile()

if __name__ == "__main__":

    inputs = {"question": "Is there any lag? If so, why isn't the consumer processing?"}
    for output in app.stream(inputs):
        for key, value in output.items():
            print(f"Node '{key}': {value['final_answer']}")
