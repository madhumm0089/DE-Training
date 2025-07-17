use day04;

select * from accounts;
update accounts set balance = balance - 100 where acc_id = 2;

start transaction; 
update accounst set balance = balance - 100 where acc_id = 2; 
select sleep(30);
update accounts set balance = balance - 200 where acc_id = 1;
commit;-- 