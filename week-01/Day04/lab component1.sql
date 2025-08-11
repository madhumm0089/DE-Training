use day04;
--- task1
with rank_emp as(
select name,department, salary, 
dense_rank() over (partition by department order by salary asc) as rownumber
from employees)
select name, department, salary, rownumber
from rank_emp
where rownumber <=3;

select * from (
 select name, department, salary, 
 dense_rank() over( partition by department order by salary asc)as ranking from employees) as new_table where ranking <= 3;

--- task2
select * from customers;
select * from orders;

SELECT c.customer_id, c. name
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id
  AND o.orderdate >= CURDATE() - INTERVAL 1 year
WHERE o.order_id IS NULL;

-- comap
select emp_id, department, salary,
lag(salary) over (partition by department order by salary) as prev_salary
from employees;

--- Calculate total salary per department
SELECT emp_id, department, salary,
 SUM(salary) OVER(PARTITION BY department) AS dept_total
FROM employees;

with dept_avg as(
select department, avg(salary) as avg_salary from employees group by department)
select * from dept_avg where avg_salary > 55000;

select department, avg(salary) as avg_salary
from employees
group by department
having avg_salary >55000;

SELECT 
    *
FROM
    (SELECT 
        department, AVG(salary) AS avg_s
    FROM
        employees
    GROUP BY department) AS new_table
WHERE
    avg_s > 55000;