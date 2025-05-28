use role sysadmin;
use warehouse adhoc_wh;
use database sandbox;
use schema consumption_sch;

-- 1. Yearly Revenue KPIs
CREATE OR REPLACE VIEW vw_yearly_revenue_kpis AS
SELECT
    d.year,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.subtotal) / COUNT(DISTINCT f.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(f.subtotal) / COUNT(f.order_item_id), 2) AS avg_revenue_per_item,
    MAX(f.subtotal) AS max_order_value
FROM order_item_fact f
JOIN date_dim d ON f.order_date_dim_key = d.date_dim_hk
WHERE f.delivery_status = 'Delivered'
GROUP BY d.year;

-- 2. Monthly Revenue KPIs
CREATE OR REPLACE VIEW vw_monthly_revenue_kpis AS
SELECT
    d.year,
    d.month,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.subtotal) / COUNT(DISTINCT f.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(f.subtotal) / COUNT(f.order_item_id), 2) AS avg_revenue_per_item,
    MAX(f.subtotal) AS max_order_value
FROM order_item_fact f
JOIN date_dim d ON f.order_date_dim_key = d.date_dim_hk
WHERE f.delivery_status = 'Delivered'
GROUP BY d.year, d.month;

-- 3. Daily Revenue KPIs
CREATE OR REPLACE VIEW vw_daily_revenue_kpis AS
SELECT
    d.year,
    d.month,
    d.day_of_the_month AS day,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.subtotal) / COUNT(DISTINCT f.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(f.subtotal) / COUNT(f.order_item_id), 2) AS avg_revenue_per_item,
    MAX(f.subtotal) AS max_order_value
FROM order_item_fact f
JOIN date_dim d ON f.order_date_dim_key = d.date_dim_hk
WHERE f.delivery_status = 'Delivered'
GROUP BY d.year, d.month, d.day_of_the_month;

-- 4. Revenue by Day of Week
CREATE OR REPLACE VIEW vw_revenue_by_weekday AS
SELECT
    d.day_name,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM order_item_fact f
JOIN date_dim d ON f.order_date_dim_key = d.date_dim_hk
WHERE f.delivery_status = 'Delivered'
GROUP BY d.day_name;

-- 5. Monthly Revenue by Restaurant
CREATE OR REPLACE VIEW vw_monthly_revenue_by_restaurant AS
SELECT
    d.year,
    d.month,
    r.name AS restaurant_name,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.subtotal) / COUNT(DISTINCT f.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(f.subtotal) / COUNT(f.order_item_id), 2) AS avg_revenue_per_item,
    MAX(f.subtotal) AS max_order_value
FROM order_item_fact f
JOIN date_dim d ON f.order_date_dim_key = d.date_dim_hk
JOIN restaurant_dim r ON f.restaurant_dim_key = r.restaurant_hk
WHERE f.delivery_status = 'Delivered'
GROUP BY d.year, d.month, r.name;

-- 6. Top Menu Items
CREATE OR REPLACE VIEW consumption_sch.vw_top_menu_items AS
SELECT
    m.Item_Name AS menu_item_name,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.subtotal) AS total_revenue_generated
FROM
    consumption_sch.order_item_fact f
JOIN
    consumption_sch.menu_dim m
    ON f.menu_dim_key = m.menu_dim_hk
GROUP BY
    m.Item_Name
ORDER BY
    total_quantity_sold DESC
LIMIT 20;


-- 7. Top Customers by Revenue
CREATE OR REPLACE VIEW vw_top_customers_by_revenue AS
SELECT
    c.customer_id,
    c.name,
    SUM(f.subtotal) AS total_spent,
    COUNT(DISTINCT f.order_id) AS order_count
FROM order_item_fact f
JOIN customer_dim c ON f.customer_dim_key = c.customer_hk
GROUP BY c.customer_id, c.name
ORDER BY total_spent DESC
LIMIT 20;

-- 8. Revenue by Locality
CREATE OR REPLACE VIEW vw_revenue_by_locality AS
SELECT
    r.locality,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM order_item_fact f
JOIN restaurant_dim r ON f.restaurant_dim_key = r.restaurant_hk
GROUP BY r.locality;

-- 9. Revenue by Cuisine Type
CREATE OR REPLACE VIEW vw_revenue_by_cuisine AS
SELECT
    r.cuisine_type,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM order_item_fact f
JOIN restaurant_dim r ON f.restaurant_dim_key = r.restaurant_hk
GROUP BY r.cuisine_type;

-- 10. New vs Returning Customers
CREATE OR REPLACE VIEW vw_customer_type_summary AS
WITH first_orders AS (
    SELECT customer_dim_key, MIN(d.calendar_date) AS first_order_date
    FROM order_item_fact f
    JOIN date_dim d ON f.order_date_dim_key = d.date_dim_hk
    GROUP BY customer_dim_key
)
SELECT
    d.year,
    COUNT(DISTINCT CASE WHEN d.calendar_date = fo.first_order_date THEN f.customer_dim_key END) AS new_customers,
    COUNT(DISTINCT CASE WHEN d.calendar_date > fo.first_order_date THEN f.customer_dim_key END) AS returning_customers
FROM order_item_fact f
JOIN date_dim d ON f.order_date_dim_key = d.date_dim_hk
JOIN first_orders fo ON f.customer_dim_key = fo.customer_dim_key
GROUP BY d.year;

-- 11. Repeat Customer Rate
CREATE OR REPLACE VIEW vw_repeat_customer_rate AS
SELECT
    ROUND(
        COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_dim_key END) 
        / NULLIF(COUNT(DISTINCT customer_dim_key), 0), 2
    ) AS repeat_customer_rate
FROM (
    SELECT customer_dim_key, COUNT(DISTINCT order_id) AS order_count
    FROM order_item_fact
    GROUP BY customer_dim_key
);

-- 12. Average Items Per Order
CREATE OR REPLACE VIEW vw_avg_items_per_order AS
SELECT
    ROUND(SUM(quantity) / COUNT(DISTINCT order_id), 2) AS avg_items_per_order
FROM order_item_fact;

-- 13. Customer Lifetime Value (CLV)
CREATE OR REPLACE VIEW vw_avg_customer_lifetime_value AS
SELECT
    ROUND(AVG(total_spent), 2) AS avg_customer_lifetime_value
FROM (
    SELECT customer_dim_key, SUM(subtotal) AS total_spent
    FROM order_item_fact
    GROUP BY customer_dim_key
);

-- 14. Revenue by Delivery Agent
CREATE OR REPLACE VIEW vw_revenue_by_delivery_agent AS
SELECT
    d.name AS delivery_agent_name,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM order_item_fact f
JOIN delivery_agent_dim d ON f.delivery_agent_dim_key = d.delivery_agent_hk
GROUP BY d.name;

-- 15. Revenue by Item Type
CREATE OR REPLACE VIEW vw_revenue_by_item_type AS
SELECT
    m.item_type,
    SUM(f.subtotal) AS total_revenue,
    COUNT(f.order_item_id) AS items_sold
FROM order_item_fact f
JOIN menu_dim m ON f.menu_dim_key = m.menu_dim_hk
GROUP BY m.item_type;

-- 16. Lost Revenue from Non-Delivered Orders
CREATE OR REPLACE VIEW vw_lost_revenue_cancelled AS
SELECT
    SUM(subtotal) AS lost_revenue
FROM order_item_fact
WHERE delivery_status != 'Delivered';

-- 17. Top Restaurant Locations
CREATE OR REPLACE VIEW vw_top_restaurant_locations AS
SELECT
    rl.city AS location_city,                        
    SUM(f.subtotal) AS total_revenue
FROM order_item_fact f
JOIN restaurant_dim r ON f.restaurant_dim_key = r.restaurant_hk
JOIN restaurant_location_dim rl ON r.location_id_fk = rl.location_id
GROUP BY rl.city
ORDER BY total_revenue DESC
LIMIT 10;


-- 18. Peak Order Day
CREATE OR REPLACE VIEW vw_peak_order_day AS
SELECT
    day_name,
    COUNT(DISTINCT order_id) AS total_orders
FROM order_item_fact f
JOIN date_dim d ON f.order_date_dim_key = d.date_dim_hk
GROUP BY day_name
ORDER BY total_orders DESC
LIMIT 1;

-- 19. Orders by Customer Gender
CREATE OR REPLACE VIEW vw_orders_by_gender AS
SELECT
    c.gender,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.subtotal) AS total_revenue
FROM order_item_fact f
JOIN customer_dim c ON f.customer_dim_key = c.customer_hk
GROUP BY c.gender;

-- 20. Menu Category Performance
CREATE OR REPLACE VIEW vw_revenue_by_menu_category AS
SELECT
    m.category,
    SUM(f.subtotal) AS total_revenue,
    COUNT(f.order_item_id) AS items_sold
FROM order_item_fact f
JOIN menu_dim m ON f.menu_dim_key = m.menu_dim_hk
GROUP BY m.category;
