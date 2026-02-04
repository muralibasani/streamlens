import os
import json
from openai import OpenAI

client = OpenAI(
    api_key=os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy"),
    base_url=os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL"),
)


def query_topology(question: str, topology: dict) -> dict:
    prompt = f"""
You are a Kafka topology reasoning assistant.
You are given a graph with nodes and edges representing Kafka topics, apps, connectors, streams, and schemas.

Current Topology Graph JSON:
{json.dumps(topology)}

User question:
{question}

Using the graph:
1. Answer in plain English.
2. Return a list of node IDs to highlight in the UI that are relevant to the answer.

Output format (JSON only):
{{
  "answer": "Plain English explanation...",
  "highlightNodes": ["topic:orders.raw", "app:checkout-svc", "connector:snowflake-sink"]
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
        print("AI Query failed", e)
        return {
            "answer": "I couldn't process your request at the moment. Please try again later.",
            "highlightNodes": [],
        }
