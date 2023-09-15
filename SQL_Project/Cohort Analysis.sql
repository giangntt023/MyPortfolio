--- PART 1 ---
/*Topic: 
You are the Business Owner of the electricity bill payment product (sub_category = electricity). In the past 2020, 
the Marketing team has implemented a lot of promotions (promotion_id <> '0') but don't know if it's effective or not? 
So you want to evaluate some of the following metrics:
●	Task 1: Tell me the trend of the number of successful payment transactions with promotion (promtion_trans) on a weekly basis 
and account for how much of the total number of successful payment transactions (promotion_ratio) ?
●	Task 2: Out of the total number of successful paying customers enjoying the promotion, how many % of customers have incurred 
any other successful payment transactions that are not promotional transactions?  */

-- Task 1:
WITH elec_trans AS (
    SELECT transaction_id,transaction_time, promotion_id
        , DATEPART(WEEK, transaction_time) Week_number
        , COUNT(transaction_id) OVER (PARTITION BY DATEPART(WEEK, transaction_time)) total_per_week -- total number of successful payment transactions per week
    FROM fact_transaction_2020 fact20
    LEFT JOIN dim_scenario AS sce ON fact20.scenario_id= sce.scenario_id
    WHERE status_id = 1 AND sub_category='Electricity'
)
, promo_trans AS ( -- number of successful payment transactions with promotion per week
    SELECT  DISTINCT Week_number
        , COUNT(CASE WHEN promotion_id != '0' THEN transaction_id END) OVER (PARTITION BY Week_number) promotion_trans
        , total_per_week
    FROM elec_trans
)
SELECT * 
    , FORMAT(promotion_trans*1.0/ total_per_week, 'p') promotion_ratio
FROM promo_trans
ORDER BY Week_number

-- Task 2:
WITH elec_trans AS (
    SELECT customer_id, transaction_id, promotion_id
        , IIF(promotion_id = '0', 'not-promo', 'promo') trans_type
        , LAG(IIF(promotion_id = '0', 'not-promo', 'promo'), 1) OVER (PARTITION BY customer_id ORDER BY transaction_id ASC) previous_tran
        , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_id ASC) row_number
    FROM fact_transaction_2020 fact20
    LEFT JOIN dim_scenario AS sce ON fact20.scenario_id= sce.scenario_id
    WHERE status_id = 1 AND sub_category='Electricity'
)
, first_promo_table AS ( -- list customer with first transaction is promotion
    SELECT DISTINCT customer_id FROM elec_trans
    WHERE row_number = 1 AND trans_type = 'promo'
)
SELECT 
    COUNT(DISTINCT first_promo_table.customer_id) number_cus
    , (SELECT COUNT(first_promo_table.customer_id) FROM first_promo_table) total_cus
    , FORMAT(COUNT(DISTINCT first_promo_table.customer_id)*1.0/
    (SELECT COUNT(first_promo_table.customer_id) FROM first_promo_table), 'p') pct
FROM first_promo_table
JOIN elec_trans ON first_promo_table.customer_id=elec_trans.customer_id
WHERE trans_type='not-promo' AND previous_tran='promo'

---------------
--- PART 2: Cohort Analysis  ---
-- 1.1.	Basic Retention Curve
-- Task 1.1A: 
/* Task A: As you know that 'Telco Card' is the most popular product in the Telco group (accounting for more than 99% of the total). 
You want to evaluate the quality of user acquisition in Jan 2019 by the retention metric. First, you need to know how many users 
are retained in each subsequent month from the first month (Jan 2019) they pay the successful transaction (only get data of 2019). */

WITH fact_trans  AS (
    SELECT transaction_id, customer_id, transaction_time
        , MONTH(MIN(transaction_time) OVER (PARTITION BY customer_id)) first_month
        , DATEDIFF(MONTH,MIN(transaction_time) OVER (PARTITION BY customer_id), transaction_time) subsequent_month 
    FROM fact_transaction_2019 fact19
    LEFT JOIN dim_scenario sce ON fact19.scenario_id = sce.scenario_id
    WHERE status_id = 1 AND sub_category = 'Telco Card'
)
SELECT subsequent_month
    , COUNT(DISTINCT customer_id) retained_users
FROM fact_trans
WHERE first_month=1
GROUP BY subsequent_month
ORDER BY subsequent_month ASC
-- Task 1.1B: 
/* You realize that the number of retained customers has decreased over time. 
Let’s calculate retention =  number of retained customers / total users of the first month. */

WITH fact_table AS(
SELECT customer_id, transaction_id, transaction_time
, MIN ( MONTH (transaction_time) ) OVER ( PARTITION BY customer_id ) AS first_month
, DATEDIFF ( month, MIN ( transaction_time ) OVER ( PARTITION BY customer_id ) , transaction_time ) AS subsequent_month
FROM fact_transaction_2019 fact_19
JOIN dim_scenario AS sce
ON fact_19.scenario_id = sce.scenario_id
WHERE sub_category = 'Telco Card'
AND status_id = 1
)
, retain_table AS (
    SELECT subsequent_month
    , COUNT (DISTINCT customer_id) AS retained_customers
    FROM fact_table
    WHERE first_month = 1
    GROUP BY subsequent_month
) 
SELECT *
    , MAX (retained_customers) OVER () original_users
    , FORMAT(retained_customers*1.0/MAX (retained_customers) OVER () ,'p') pct_retained
FROM retain_table

-- 1.2.	Cohorts Derived from the Time Series Itself
-- Task 1.2A:
-- Expand your previous query to calculate retention for multi attributes from the acquisition month (first month) (from Jan to December).

WITH fact_trans  AS (
    SELECT transaction_id, customer_id, transaction_time
        ,  MONTH(MIN(transaction_time) OVER (PARTITION BY customer_id )) acquisition_month
        , DATEDIFF(MONTH, MIN(transaction_time) OVER (PARTITION BY customer_id),transaction_time  ) subsequent_month
    FROM fact_transaction_2019 fact19
    LEFT JOIN dim_scenario sce ON fact19.scenario_id = sce.scenario_id
    WHERE status_id = 1 AND sub_category = 'Telco Card' 
)
, retained_table AS (
    SELECT acquisition_month
        , subsequent_month
        , COUNT(DISTINCT customer_id) retained_users
    FROM fact_trans
    GROUP BY acquisition_month, subsequent_month
)
SELECT *
    , MAX(retained_users) OVER(PARTITION BY acquisition_month) original_users
    , FORMAT(retained_users*1.0/MAX(retained_users) OVER(PARTITION BY acquisition_month), 'p') pct_retained
FROM retained_table


-- Task 1.2B: Task B: Then modify the result following by pivot table: 
WITH fact_trans  AS (
    SELECT transaction_id, customer_id, MONTH(transaction_time) month
        , MONTH(MIN(transaction_time) OVER (PARTITION BY customer_id )) acquisition_month
        , DATEDIFF(MONTH, MIN(transaction_time) OVER (PARTITION BY customer_id),transaction_time  ) subsequent_month
    FROM fact_transaction_2019 fact19
    LEFT JOIN dim_scenario sce ON fact19.scenario_id = sce.scenario_id
    WHERE status_id = 1 AND sub_category = 'Telco Card' 
)
, retained_table AS (
    SELECT acquisition_month, subsequent_month
        , COUNT(DISTINCT customer_id) retained_users
    FROM fact_trans
    GROUP BY acquisition_month, subsequent_month
)
, pct_table AS(
    SELECT *
        , MAX(retained_users) OVER(PARTITION BY acquisition_month) original_users
        , FORMAT(retained_users*1.0/MAX(retained_users) OVER(PARTITION BY acquisition_month), 'p') pct_retained
    FROM retained_table
)
SELECT acquisition_month, original_users
    , "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"
FROM (
SELECT acquisition_month, subsequent_month, original_users, pct_retained -- STRING
FROM pct_table
) AS source_table
PIVOT (
    MAX(pct_retained)
    FOR subsequent_month IN( "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11")
 ) AS pivot_table
 ORDER BY acquisition_month







    
