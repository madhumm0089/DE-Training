create database if not exists day10;
use day10;

--- hub Customers, Products
drop table if exists customer_hub;
drop table if exists product_hub;

create table customer_hub(
	customer_hashkey varchar(100) primary key,
	customer_id varchar(10),
    loadDate timestamp,
    record_source varchar(100)
    );
    
create table product_hub(
	product_hashkey varchar(100) primary key,
    product_id varchar(100),
	loadDate timestamp,
    record_source varchar(100)
    );
    
    
--- Links purchase

CREATE TABLE link_purchase (
    customer_hashkey VARCHAR(100),
    product_hashkey VARCHAR(100),
    loadDate TIMESTAMP,
    record_source VARCHAR(100),
    PRIMARY KEY (customer_hashkey, product_hashkey),
    FOREIGN KEY (customer_hashkey) REFERENCES customer_hub(customer_hashkey),
    FOREIGN KEY (product_hashkey) REFERENCES product_hub(product_hashkey)
);


--- satellite

CREATE TABLE sat_customer_details (
    customer_hashkey VARCHAR(100),
    loadDate TIMESTAMP,
    name VARCHAR(100),
    address VARCHAR(200),
    contact VARCHAR(100),
    record_source VARCHAR(100),
    PRIMARY KEY (customer_hashkey, loadDate),
    FOREIGN KEY (customer_hashkey) REFERENCES customer_hub(customer_hashkey)
);

CREATE TABLE sat_product_details (
    product_hashkey VARCHAR(100),
    loadDate TIMESTAMP,
    name VARCHAR(100),
    category VARCHAR(100),
    price DECIMAL(18,2),
    record_source VARCHAR(100),
    PRIMARY KEY (product_hashkey, loadDate),
    FOREIGN KEY (product_hashkey) REFERENCES product_hub(product_hashkey)
);

CREATE TABLE sat_purchase_details (
    customer_hashkey VARCHAR(100),
    product_hashkey VARCHAR(100),
    loadDate TIMESTAMP,
    purchase_date DATE,
    quantity INT,
    sales_amount DECIMAL(18,2),
    record_source VARCHAR(100),
    PRIMARY KEY (customer_hashkey, product_hashkey, loadDate),
    FOREIGN KEY (customer_hashkey, product_hashkey) REFERENCES link_purchase(customer_hashkey, product_hashkey)
);

    
    