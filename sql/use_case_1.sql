/*
Author: Justine Paul Padayao
Description: 
This SQL query identifies the most popular engagement day and time for a specific campaign, based on the `reward_transaction` table data.
The engagement data is stored in UTC format, and the query converts it to Singapore Time (SGT).
The data is filtered by a specific date range and campaign ID. 

Tables:
    - reward_transaction: Contains information on reward transactions or events, including the `updated_at` timestamp and associated `reward_campaign_id`.
    - reward_campaign: Contains information about rewards, including the `reward_name` and associated `campaign_id`.
    - campaign: Contains basic information about campaigns, including `Id`, `name`, and `status`.

Fields:
    - reward_transaction.id: Represents the reward transaction.
    - reward_transaction.updated_at: The timestamp of the reward event, stored in UTC.
    - reward_campaign.campaign_id: The foreign key referencing the campaign associated with the reward.
    - campaign.Id: The unique identifier for a campaign.
    
Use Case:
    This query helps Client X determine the most popular day and time for reward transactions (engagement) for a specific campaign
    within a defined date range.

Sample Input:
    - campaign_id: 1
    - start_date: '2019-08-01'
    - end_date: '2019-08-31'

Expected Output:
    - no_of_transactions: The number of reward transactions (engagements).
    - date: The date of the engagement in SGT.
    - time: The hour of engagement in SGT.
    - campaign_id: The ID of the campaign.
    - reward_campaign_id: The ID of the reward campaign.
*/

SELECT 
    COUNT(rt.id) AS `No. of Transactions`, 
    DATE(CONVERT_TIMEZONE('UTC', 'Asia/Singapore', rt.updated_at)) AS `Date`,
    DATE_PART('hour', CONVERT_TIMEZONE('UTC', 'Asia/Singapore', rt.updated_at)) AS `Time`
FROM 
    test.perx_schema.reward_transaction rt
JOIN 
    test.perx_schema.reward_campaign rc ON rt.reward_campaign_id = rc.id
JOIN 
    test.perx_schema.campaign c ON rc.campaign_id = c.Id
WHERE 
    c.Id = <campaign_id>
    AND rt.updated_at BETWEEN '<start_date>' AND '<end_date>'
GROUP BY 
    `Date`, `Time`
ORDER BY 
    `No. of Transactions` DESC;

/*
Sample Implementation:

To run the query, replace the following parameters:
- <campaign_id>: The ID of the campaign you want to analyze (e.g., 1).
- <start_date> and <end_date>: The date range for analysis (e.g., '2019-08-01' AND '2019-08-31').

Example:

SELECT 
    COUNT(rt.id) AS `No. of Transactions`, 
    DATE(CONVERT_TIMEZONE('UTC', 'Asia/Singapore', rt.updated_at)) AS `Date`,
    DATE_PART('hour', CONVERT_TIMEZONE('UTC', 'Asia/Singapore', rt.updated_at)) AS `Time`
FROM 
    test.perx_schema.reward_transaction rt
JOIN 
    test.perx_schema.reward_campaign rc ON rt.reward_campaign_id = rc.id
JOIN 
    test.perx_schema.campaign c ON rc.campaign_id = c.Id
WHERE 
    c.Id = 1
    AND rt.updated_at BETWEEN '2019-08-01' AND '2019-08-31'
GROUP BY 
    `Date`, `Time`
ORDER BY 
    `No. of Transactions` DESC;
*/
