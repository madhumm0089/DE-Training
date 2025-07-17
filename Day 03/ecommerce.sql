select name,salary, count(*) as highest_salary
from employees
where department = 'Engineering' and salary > 50000
group by name, salary;

select * from employees;

select name,salary
from employees
where department = 'HR';

select name,salary
from employees
where salary > 70000;

select distinct department
from employees;

select department, avg(salary) as average_salary
from employees
group by department;

select department, count(*) as num_of_emp
from employees
group by department;

select department, sum(salary) as total_salary
from employees
where department = 'Engineering';

select name
from employees
where manager_id= 101;

select name
from employees
where name like 'A%';

select *
from employees
where salary = (select max(salary) from employees);

select *
from employees
where salary > (select avg(salary) from employees);

select department
from employees;

select department
from employees 
group by department
having count(*) > 1;

