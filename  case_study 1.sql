CREATE DATABASE operational_analytics;
USE operational_analytics;
CREATE TABLE job_data (
    job_id INT,
    actor_id INT,
    event VARCHAR(50),
    language VARCHAR(50),
    time_spent FLOAT,
    org VARCHAR(100),
    ds DATE
);
SELECT 
    STR_TO_DATE(ds, '%m-%d-%Y') AS review_date,
    COUNT(job_id) AS jobs_reviewed
FROM 
    job_data
WHERE 
    STR_TO_DATE(ds, '%m-%d-%Y') BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY 
    review_date
ORDER BY 
    review_date;
SELECT 
    ds AS review_date,
    COUNT(*) AS total_events,
    SUM(time_spent) AS total_time_spent_seconds,
    ROUND(COUNT(*) / NULLIF(SUM(time_spent), 0), 4) AS throughput_per_sec,
    ROUND(AVG(COUNT(*) / NULLIF(SUM(time_spent), 0)) 
        OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 4) AS rolling_avg_throughput
FROM 
    job_data
GROUP BY 
    ds
ORDER BY 
    ds;

SELECT 
    language,
    COUNT(*) AS total_reviews,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS language_percentage
FROM 
    job_data
WHERE 
    ds >= (SELECT MAX(ds) FROM job_data) - INTERVAL 30 DAY
GROUP BY 
    language
ORDER BY 
    language_percentage DESC;

SELECT 
    job_id, actor_id, event, language, time_spent, org, ds, 
    COUNT(*) AS duplicate_count
FROM 
    job_data
GROUP BY 
    job_id, actor_id, event, language, time_spent, org, ds
HAVING 
    COUNT(*) > 1;

#TASK -- 1

SELECT 
    STR_TO_DATE(ds, '%m/%d/%Y') AS review_date,
    COUNT(job_id) AS jobs_reviewed
FROM 
    job_data
WHERE 
    STR_TO_DATE(ds, '%m/%d/%Y') BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY 
    review_date
ORDER BY 
    review_date;





#Task 2: Throughput 7-Day Rolling Average

SELECT 
    review_date,
    total_events,
    total_time_spent,
    ROUND(total_events / NULLIF(total_time_spent, 0), 4) AS throughput_per_sec,
    ROUND(AVG(total_events / NULLIF(total_time_spent, 0)) 
          OVER (ORDER BY review_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 4) AS rolling_avg_throughput
FROM (
    SELECT 
        STR_TO_DATE(ds, '%m/%d/%Y') AS review_date,
        COUNT(*) AS total_events,
        SUM(time_spent) AS total_time_spent
    FROM 
        job_data
    GROUP BY 
        review_date
) subquery
ORDER BY 
    review_date;



    #Task 3: Calculate Percentage Share of Each Language (Last 30 Days)
    
    SELECT 
    language,
    COUNT(*) AS total_reviews,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS language_percentage
FROM 
    job_data
WHERE 
    STR_TO_DATE(ds, '%m/%d/%Y') BETWEEN '2020-11-25' AND '2020-11-30'
GROUP BY 
    language
ORDER BY 
    language_percentage DESC;

#Task 4: Duplicate Row Detection

SELECT 
    job_id, actor_id, event, language, time_spent, org, ds, 
    COUNT(*) AS occurrence_count
FROM 
    job_data
GROUP BY 
    job_id, actor_id, event, language, time_spent, org, ds
HAVING 
    COUNT(*) > 1
ORDER BY 
    occurrence_count DESC;

#CASE STUDY 2

CREATE TABLE users (
    user_id INT PRIMARY KEY,
    created_at DATETIME,
    company_id INT,
    language VARCHAR(50),
    activated_at DATETIME,
    state VARCHAR(50)
);
CREATE TABLE events (
    event_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    occurred_at DATETIME,
    event_type VARCHAR(100),
    event_name VARCHAR(100),
    location VARCHAR(100),
    device VARCHAR(100),
    user_type VARCHAR(100),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);


CREATE TABLE email_events (
    email_event_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    occurred_at DATETIME,
    action VARCHAR(100),
    user_type VARCHAR(100),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

#user engagement
SELECT * FROM events LIMIT 10;
INSERT INTO users (user_id, created_at, company_id, language, activated_at, state)
VALUES 
(1, '2023-03-01', 101, 'English', '2023-03-02', 'Active'),
(2, '2023-03-05', 102, 'English', '2023-03-06', 'Active');

INSERT INTO events (user_id, occurred_at, event_type, event_name, location, device, user_type)
VALUES 
(1, '2023-04-01 10:00:00', 'login', 'User Login', 'New York', 'Mobile', 'Free'),
(2, '2023-04-03 15:30:00', 'search', 'Job Search', 'California', 'Desktop', 'Premium'),
(1, '2023-04-10 11:20:00', 'logout', 'User Logout', 'New York', 'Mobile', 'Free');

SELECT
    YEARWEEK(occurred_at, 1) AS year_week,
    COUNT(DISTINCT user_id) AS active_users
FROM events
GROUP BY year_week
ORDER BY year_week;


#SQL Query for User Growth

SELECT
    YEAR(created_at) AS signup_year,
    MONTH(created_at) AS signup_month,
    COUNT(DISTINCT user_id) AS new_users
FROM users
GROUP BY signup_year, signup_month
ORDER BY signup_year, signup_month;

#SQL for Weekly Retention

WITH cohorts AS (
    SELECT
        user_id,
        DATE(created_at) AS signup_date,
        YEARWEEK(created_at, 1) AS signup_week
    FROM users
)

SELECT
    c.signup_week,
    YEARWEEK(e.occurred_at, 1) AS activity_week,
    COUNT(DISTINCT e.user_id) AS retained_users
FROM cohorts c
JOIN events e ON c.user_id = e.user_id
GROUP BY c.signup_week, activity_week
ORDER BY c.signup_week, activity_week;

#Weekly Engagement Per Device
SELECT
    YEARWEEK(occurred_at, 1) AS year_week,
    device,
    COUNT(DISTINCT user_id) AS active_users
FROM events
GROUP BY year_week, device
ORDER BY year_week, device;

#Email Engagement Analysis
INSERT INTO email_events (user_id, occurred_at, action, user_type)
VALUES
(1, '2023-04-02 12:00:00', 'delivered', 'Free'),
(2, '2023-04-02 12:10:00', 'opened', 'Premium'),
(1, '2023-04-03 15:00:00', 'clicked', 'Free'),
(2, '2023-04-04 16:30:00', 'delivered', 'Premium'),
(1, '2023-04-05 11:00:00', 'opened', 'Free');

SELECT
    action AS email_event_type,
    COUNT(*) AS total_events
FROM email_events
GROUP BY email_event_type
ORDER BY total_events DESC;

