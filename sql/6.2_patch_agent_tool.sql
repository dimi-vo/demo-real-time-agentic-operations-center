-- 6.2 The patch agent as a tool: a connection to your own patch-agent, and a tool an agent can call.
-- The patch agent is the external agent of this section: a LangGraph graph on your laptop,
-- served over A2A (agent to agent) by `uv run patch-agent`. CREATE CONNECTION tells
-- Confluent Cloud where it is, and CREATE TOOL makes it a tool that an agent names in USING
-- TOOLS, as the security agent does in 6.3. Run the two statements in order, one at a time:
-- the connection, then the tool.
-- Start `uv run patch-agent --public-url <tunnel URL>` first, in a terminal of its own. It
-- prints this file with your URL filled in, ready to paste, then a block for every call.
-- TODO (tunnel): which tunnel to open in front of port 8001 is not decided yet.
-- Flink requires a username and a password on an A2A connection. patch-agent checks neither.
-- max_retries is 0 because a retry would patch twice. request_timeout (30 s) covers the
-- patch agent taking 4 to 10 s.
-- A new tunnel URL means DROP TOOL patch_agent_tool, then DROP CONNECTION
-- patch_agent_connection, then both statements again: ALTER CONNECTION changes only the
-- credentials, never the endpoint.
CREATE CONNECTION `patch_agent_connection`
WITH (
  'type' = 'a2a',
  'endpoint' = '<your patch-agent URL>',
  'username' = 'workshop',
  'password' = 'workshop'
);

CREATE TOOL `patch_agent_tool`
USING CONNECTION `patch_agent_connection`
WITH (
  'type' = 'a2a',
  'agent_card_path' = '/.well-known/agent-card.json',
  'request_timeout' = '30',
  'max_retries' = '0'
);
