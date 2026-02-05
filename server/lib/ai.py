import os
import json
from openai import OpenAI

client = OpenAI(
    api_key=os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy"),
    base_url=os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL"),
)


def query_topology(question: str, topology: dict) -> dict:
    prompt = f"""
You are StreamPilot, a Kafka topology reasoning assistant.
You are given a graph with nodes and edges representing Kafka topics, producers, consumers, streams applications, connectors, and schemas.

Current Topology Graph JSON:
{json.dumps(topology)}

User question:
{question}

Using the graph:
1. Analyze the nodes and edges to find relevant entities that answer the question.
2. Provide a clear, concise answer in plain English.
3. Return the exact node IDs (from the graph) of all relevant entities to highlight and zoom to in the UI.

Important:
- For questions about "producers writing to X topic", include producer node IDs and the topic node ID
- For questions about "consumers of X topic", include consumer node IDs and the topic node ID
- For questions about "topics produced by X", include the producer/app node ID and all relevant topic node IDs
- For questions about schemas, include schema node IDs (format: "schema:subject-name") and their linked topic nodes
- For questions about connectors, include connector node IDs (format: "connect:connector-name") and their linked topic nodes
- For questions about ACLs on a topic, include ACL node IDs (format: "acl:topic:topicname") and the topic node
- Always include the full node ID as it appears in the graph (e.g., "topic:testtopic", "group:mygroup", "jmx:active-producer:testtopic", "schema:orders-value", "connect:file-source", "acl:topic:transactions-topic")

Output format (JSON only):
{{
  "answer": "Plain English explanation...",
  "highlightNodes": ["topic:orders", "group:checkout-consumer", "jmx:active-producer:orders", "connect:jdbc-sink", "schema:orders-value"]
}}
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful Kafka expert."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content or "{}"
        return json.loads(content)
    except Exception as e:
        print(f"AI Query failed: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        
        # Check if it's an API key issue
        api_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy")
        if api_key == "dummy" or not api_key or api_key.startswith("sk-") is False:
            error_msg = "AI assistant not configured. Please set AI_INTEGRATIONS_OPENAI_API_KEY environment variable. See docs/AI_SETUP.md for details."
        else:
            error_msg = f"I couldn't process your request: {str(e)}"
        
        return {
            "answer": error_msg,
            "highlightNodes": [],
        }
