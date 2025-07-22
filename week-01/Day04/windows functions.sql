use day04;

select * from employees;
--- rank function
select department, salary,
rank() over(partition by department order by salary asc)
from employees;

--- dense rank function
select department, salary,
dense_rank() over(partition by department order by salary asc)
from employees;

--- row number function
select department, salary,
row_number() over(partition by department order by salary asc)
from employees;

--- salary diffrence
select department,salary,
(lag(salary) over(partition by department order by salary asc) - lead(salary) over(partition by department order by salary asc)) as salary_diff_to_prev
from employees;

--- total salary of each department
select department, sum(salary)
over(partition by department order by salary)
from employees;




