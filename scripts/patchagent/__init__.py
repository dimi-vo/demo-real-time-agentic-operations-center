"""`uv run patch-agent`: the external agent of section 6, on the participant's laptop (D-070).

A LangGraph graph with one node and no model, served over A2A. Confluent Cloud's
security agent (lab 6.3) calls it through lab 6.2's connection and tool when a critical
finding should be patched; it waits a few seconds and answers with a commit. Read in this
order: `reply.py` (the request line and the reply, no I/O), `graph.py` (the node),
`executor.py` (the A2A task around the graph), `app.py` (the agent card, the server, the
CLI).
"""
