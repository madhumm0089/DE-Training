use day04;
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

select * from accounts;
DELIMITER $$
CREATE TRIGGER prevent_negative_balance
BEFORE UPDATE ON accounts
FOR EACH ROW
BEGIN
 IF NEW.balance < 0 THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Insufficient balance!';
 END IF;
END$$
DELIMITER 

UPDATE accounts SET balance = -500 WHERE acc_id = 2;

