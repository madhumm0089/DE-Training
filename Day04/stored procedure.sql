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

DELIMITER $$

CREATE PROCEDURE insertemployees(IN empnames TEXT)
BEGIN
    DECLARE name TEXT;
    DECLARE remaining TEXT;
    DECLARE comma_pos INT;

    -- Initialize the remaining string to the input
    SET remaining = empnames;

    -- Start loop
    WHILE LENGTH(remaining) > 0 DO
        -- Find the position of the first comma
        SET comma_pos = LOCATE(',', remaining);

        IF comma_pos = 0 THEN
            -- No more commas, take the rest as the last name
            SET name = TRIM(remaining);
            SET remaining = '';
        ELSE
            -- Extract name up to comma
            SET name = TRIM(SUBSTRING(remaining, 1, comma_pos - 1));
            SET remaining = SUBSTRING(remaining, comma_pos + 1);
        END IF;

        -- Insert the employee name
        INSERT INTO sample (employeename) VALUES (name);

        -- Set empid as 'EMP' + last inserted id
        UPDATE sample 
        SET empid = CONCAT('EMP', LAST_INSERT_ID())
        WHERE id = LAST_INSERT_ID();
    END WHILE;
END$$

DELIMITER ;
drop procedure insertemployee;
