use day04;

select * from sales;

SELECT
  DATE_FORMAT(sale_date, '%Y-%m') AS month,
  SUM(amount) AS monthly_revenue,
  SUM(SUM(amount)) OVER (ORDER BY DATE_FORMAT(sale_date, '%Y-%m')) AS cumulative_revenue
FROM sales
GROUP BY month
ORDER BY month;