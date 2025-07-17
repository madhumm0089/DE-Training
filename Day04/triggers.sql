
DELIMITER $$
create trigger log_salary_change
after update on employees
for each row

begin

insert into audit_log(emp_id, old_salary, new_salary) values(old.emp_id, old.salary, new.salary);
end;
DELIMITER;

select * from employees;


update employees set salary= 10 where emp_id=1;

select * from audit_log;
