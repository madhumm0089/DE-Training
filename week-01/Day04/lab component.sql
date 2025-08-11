DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
  emp_id INT PRIMARY KEY,
  name VARCHAR(50),
  department VARCHAR(50),
  salary INT
);

INSERT INTO employees (emp_id, name, department, salary) VALUES
(1, 'Alice', 'HR', 50000),
(2, 'Bob', 'HR', 60000),
(3, 'Charlie', 'IT', 80000),
(4, 'David', 'IT', 75000),
(5, 'Eve', 'HR', 55000);

DROP TABLE IF EXISTS new_employees;
CREATE TABLE new_employees (
  emp_id INT,
  name VARCHAR(50),
  salary INT
);

INSERT INTO new_employees (emp_id, name, salary) VALUES
(1, 'Alice', 51000),
(6, 'Frank', 62000);


DROP TABLE IF EXISTS customers;
-- Create customers table
CREATE TABLE customers (
  customer_id INT PRIMARY KEY,
  name VARCHAR(50),
  registered_on DATE
);

INSERT INTO customers (customer_id, name, registered_on) VALUES
(1, 'Alice', '2022-01-15'),
(2, 'Bob', '2023-03-10'),
(3, 'Charlie', '2021-11-25'),
(4, 'Diana', '2024-02-05'),
(5, 'Evan', '2020-08-30');

-- Create orders table
CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  customer_id INT,
  orderdate DATE,
  CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


INSERT INTO customers (name) VALUES
('Alice'), ('Bob'), ('Charlie');

DROP TABLE IF EXISTS suppliers;
CREATE TABLE suppliers (
  name VARCHAR(50)
);

INSERT INTO suppliers (name) VALUES
('Bob'), ('David');

DROP TABLE IF EXISTS orders; 



INSERT INTO orders (order_id, customer_id, orderdate) VALUES
(101, 1, '2024-01-10'),
(102, 2, '2024-02-15'),
(103, 1, '2024-03-20'),
(104, 3, '2024-04-25'),
(105, 2, '2023-05-05'),
(106, 4, '2023-06-10'),
(107, 1, '2022-07-15');


DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
  acc_id INT PRIMARY KEY,
  balance INT
);

INSERT INTO accounts (acc_id, balance) VALUES
(1, 1000),
(2, 1000);
DROP table if exists sales;
CREATE TABLE sales (
  sale_id INT PRIMARY KEY,
  sale_date DATE,
  amount DECIMAL(10,2)
);

INSERT INTO sales VALUES
(1, '2024-01-05', 1500),
(2, '2024-01-20', 2200),
(3, '2024-02-10', 3000),
(4, '2024-03-12', 3500),
(5, '2024-03-20', 1800);



DROP TABLE IF EXISTS audit_log;
CREATE TABLE audit_log (
  emp_id INT,
  old_salary INT,
  new_salary INT,
  changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

use Day04;
select * from employees;

select department, salary, 
dense_rank() over (partition by department order by salary) as rownumber
from employees;

select emp_id, avg (salary) as avg_salary
from employees
group by department;

select department, avg(salary) 
over(partition by department) 
from employees;



