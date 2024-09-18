/*
Author: Justine Paul Padayao
Description: 
This SQL query provides a weekly breakdown of reward performance for Client X, showing the number of rewards redeemed 
and the percentage difference compared to the previous week.

Tables:
    - reward_transaction: Contains information on reward transactions or events, including the `updated_at` timestamp and associated `reward_campaign_id`.
    - reward_campaign: Contains information about rewards, including the `reward_name` and associated `campaign_id`.

Fields:
    - reward_transaction.updated_at: The timestamp of the reward event, stored in UTC.
    - reward_campaign.reward_name: The name of the reward associated with the transaction.
    
Use Case:
    This query helps Client X see the weekly reward performance and compare the redeemed count with the previous week.

Expected Output:
    - `Reward Name`: The name of the reward.
    - `Reward Redeemed Count`: The number of rewards redeemed during the week.
    - `Percentage Difference as Per Previous Week`: The percentage change in the number of rewards redeemed compared to the previous week.
    - `Date/Week`: The week in which the rewards were redeemed.
*/

WITH weekly_data AS (
    SELECT 
        rc.reward_name, 
        COUNT(rt.id) AS reward_redeemed_count, 
        EXTRACT(YEAR FROM rt.updated_at) AS year,
        EXTRACT(WEEK FROM rt.updated_at) AS week, 
        DATE_TRUNC('WEEK', rt.updated_at) AS week_start
    FROM 
        test.perx_schema.reward_transaction rt
    JOIN 
        test.perx_schema.reward_campaign rc ON rt.reward_campaign_id = rc.id
    GROUP BY 
        rc.reward_name, year, week, week_start
)
SELECT 
    reward_name AS `Reward Name`, 
    reward_redeemed_count AS `Reward Redeemed Count`, 
    ROUND(
        ((reward_redeemed_count - LAG(reward_redeemed_count, 1) OVER (PARTITION BY reward_name ORDER BY week_start)) 
        / NULLIF(LAG(reward_redeemed_count, 1) OVER (PARTITION BY reward_name ORDER BY week_start), 0)) * 100, 2
    ) AS `Percentage Difference as Per Previous Week`,
    week_start AS `Date/Week`
FROM 
    weekly_data
ORDER BY 
    reward_name, week_start;

/*
Sample Output:

`Reward Name`    | `Reward Redeemed Count` | `Percentage Difference as Per Previous Week` | `Date/Week`
------------------------------------------------------------------------------------------------------
My Reward        | 7                       | null                                         | 2019-08-12T00:00:00Z
My Reward        | 3                       | -57.14                                       | 2019-08-19T00:00:00Z
*/
