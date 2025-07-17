create table sample(id int primary key identity(1,1), empid nvarchar(10), employeename varchar(2000));
last_insert_id()
alter procedure insertemployee @empname nvarchar(1000)
as
begin
insert into sample(employeename) values(@empname);
update sample set empid=concat('EMP', @@IDENTITY) where id=@@IDENTITY;

end;

select * from sample;

CREATE TABLE sample (
  id INT AUTO_INCREMENT PRIMARY KEY,
  empid VARCHAR(20),
  employeename VARCHAR(2000)
);

DELIMITER $$

CREATE PROCEDURE insertemployee(IN empname VARCHAR(1000))
BEGIN
  INSERT INTO sample (employeename) VALUES (empname);
  UPDATE sample 
  SET empid = CONCAT('EMP', LAST_INSERT_ID()) 
  WHERE id = LAST_INSERT_ID();
END$$

DELIMITER ;

call insertemployee('mahesh,kishan,yogi');