/*
Author: Justine Paul Padayao
Description:
This SQL script processes HTTP log data to identify user sessions, aggregate campaign views and reward issuances, 
and checks if rewards issued were driven by campaign views. The report includes the following fields:
- user_id
- session_start (timestamp): The start time of the session.
- session_end (timestamp): The end time of the session.
- campaigns: List of campaigns the user viewed during the session.
- rewards_issued: List of rewards the user received during the session.
- reward_driven_by_campaign_view: True if a reward was issued after the user viewed a campaign containing the reward within the same session.

Session Definition:
A session is defined as user activity where the time gap between consecutive requests is less than 5 minutes.

Tables Used:
- test.perx_schema.http_log: Contains HTTP log data including timestamp, HTTP method, and request path.
- test.perx_schema.campaign_reward_mapping: Contains the mapping of campaigns to rewards.

*/

WITH session_data AS (
  SELECT
    user_id,
    timestamp,
    http_method,
    http_path,
    LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS prev_timestamp,
    CASE 
      WHEN unix_timestamp(timestamp) - unix_timestamp(LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp)) > 300 THEN 1
      ELSE 0
    END AS new_session
  FROM test.perx_schema.http_log
),
session_with_ids AS (
  SELECT 
    user_id,
    timestamp,
    http_method,
    http_path,
    SUM(new_session) OVER (PARTITION BY user_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
  FROM session_data
),
campaign_requests AS (
  SELECT
    session_id,
    user_id,
    REGEXP_EXTRACT(http_path, '/campaigns/([0-9]+)', 1) AS campaign_id
  FROM session_with_ids
  WHERE http_method = 'GET' AND http_path LIKE '/campaigns/%'
),
reward_requests AS (
  SELECT
    session_id,
    user_id,
    REGEXP_EXTRACT(http_path, '/rewards/([0-9]+)', 1) AS reward_id
  FROM session_with_ids
  WHERE http_method = 'POST' AND http_path LIKE '/rewards/%'
),
session_agg AS (
  SELECT
    s.user_id,
    s.session_id,
    MIN(s.timestamp) AS session_start,
    MAX(s.timestamp) AS session_end,
    COLLECT_SET(c.campaign_id) AS campaigns,
    COLLECT_SET(r.reward_id) AS rewards_issued
  FROM session_with_ids s
  LEFT JOIN campaign_requests c ON s.session_id = c.session_id AND s.user_id = c.user_id
  LEFT JOIN reward_requests r ON s.session_id = r.session_id AND s.user_id = r.user_id
  GROUP BY s.user_id, s.session_id
)
SELECT
  user_id,
  session_start,
  session_end,
  campaigns,
  rewards_issued,
  CASE 
    WHEN EXISTS (
      SELECT 1 
      FROM test.perx_schema.campaign_reward_mapping crm
      WHERE ARRAY_CONTAINS(rewards_issued, crm.reward_id) 
        AND ARRAY_CONTAINS(campaigns, crm.campaign_id)
    ) THEN true
    ELSE false
  END AS reward_driven_by_campaign_view
FROM session_agg;
