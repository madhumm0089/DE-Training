--- view
use day04;

create view sales_summary as
select *
from orders;

select * from sales_summary;

CREATE MATERIALIZED VIEW high_salary_departments AS
SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department;
