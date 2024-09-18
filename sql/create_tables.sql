-- Create campaign table
CREATE TABLE IF NOT EXISTS test.perx_schema.campaign (
    id BIGINT,
    name STRING,
    updated_at TIMESTAMP,
    PRIMARY KEY (id)
) USING DELTA;

-- Create reward_campaign table
CREATE TABLE IF NOT EXISTS test.perx_schema.reward_campaign (
    id BIGINT,
    Reward_name STRING,
    updated_at TIMESTAMP,
    campaign_id BIGINT,
    FOREIGN KEY (campaign_id) REFERENCES test.perx_schema.campaign(id)
) USING DELTA;

-- Create reward_transaction table
CREATE TABLE IF NOT EXISTS test.perx_schema.reward_transaction (
    id BIGINT,
    status STRING,
    updated_at TIMESTAMP,
    Reward_camapigns_id BIGINT,
    FOREIGN KEY (Reward_camapigns_id) REFERENCES test.perx_schema.reward_campaign(id)
) USING DELTA;
