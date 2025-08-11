create table Customer(
cus_id int primary key,
name varchar(100),
age int);

create table Products(
product_id int primary key,
pro_name varchar(100),
price int);

CREATE TABLE Transactions (
    trans_id INT PRIMARY KEY,
    cus_id INT,
    product_id INT,
    amount INT,
    transaction_date DATE,
    FOREIGN KEY (cus_id) REFERENCES Customer(cus_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

INSERT INTO Customer (cus_id, name, age) VALUES
(1, 'Alice Smith', 30),
(2, 'Bob Johnson', 45),
(3, 'Carol Taylor', 28);

INSERT INTO Products (product_id, pro_name, price) VALUES
(101, 'Laptop', 1200),
(102, 'Smartphone', 800),
(103, 'Headphones', 150);

INSERT INTO Transactions (trans_id, cus_id, product_id, amount, transaction_date) VALUES
(1001, 1, 101, 1200, '2025-08-01'),
(1002, 2, 103, 150, '2025-08-03'),
(1003, 3, 102, 800, '2025-08-05');



select * from dbo.Customer;
select * from dbo.Products;
select * from dbo.Transactions;