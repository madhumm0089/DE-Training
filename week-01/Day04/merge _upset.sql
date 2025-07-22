use day04;

select name as unionname from(select name from customers
union
select name from suppliers) as final;

select name from customers
expect
select name from suppliers;

select * from new_employees;

--- merge new data
merge into employees as target
using new_employees as source
on target.emp_id = source.emp_id
when matched then
update set salary= source.salary
when not matched then
(insert (emp_id, name,salary)
values(source.emp_id, source.name, source.salary);

insert into employees(emp_id, name, department) values
(101, 'madhu', 'IT'), (1,'m','sales') as new
on duplicate KEY update
name = new.name,
department= new.department;

insert into employees(emp_id, name, department) values
(101, 'madhu', 'IT'), (1,'m','sales') ,(111,'YOGI', 'HA')
on duplicate KEY update
name = VALUES(name),
department= VALUES(department);

select * from employees;