WITH total_sales AS (
        SELECT
            buyer_org_id AS org_acct_uuid,
            COUNT(product_uuid) AS total_prod_sold
        FROM cust_activity.sales_summary
        WHERE EXTRACT(YEAR FROM sale_date) = 2018
          AND prod_class_id = 4
        GROUP BY buyer_org_id
    ),
    ordered_sales AS (
        SELECT
            org_acct_uuid,
            total_prod_sold,
            SUM(total_prod_sold) OVER () AS total_prod_sold_sum,
            SUM(total_prod_sold) OVER (
                ORDER BY total_prod_sold DESC, org_acct_uuid ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_sales
        FROM total_sales
    ),
    cumulative_percentage AS (
        SELECT
            *,
            cumulative_sales / total_prod_sold_sum AS cumulative_percentage
        FROM ordered_sales
    )
SELECT
    org_acct_uuid,
    total_prod_sold,
    total_prod_sold_sum,
    cumulative_sales,
    ROUND(cumulative_percentage, 10) AS cumulative_percentage,
    CASE
        WHEN total_prod_sold = 0 THEN 'DarkDealer'
        WHEN cumulative_percentage <= 0.5 THEN 'A'
        WHEN cumulative_percentage <= 0.95 THEN 'B'
        ELSE 'C'
    END AS segmentation
FROM cumulative_percentage
ORDER BY cumulative_sales, total_prod_sold DESC, org_acct_uuid;
