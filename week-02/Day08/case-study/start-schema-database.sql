-- Step 1: Create and use the database
CREATE DATABASE IF NOT EXISTS day8morning;
USE day8morning;

DROP TABLE IF EXISTS fact_returns;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS customer_dim;
DROP TABLE IF EXISTS product_dim;
DROP TABLE IF EXISTS store_dim;
DROP TABLE IF EXISTS region_dim;

-- Step 2: Create dimension tables

-- DIMENSION: region_dim
CREATE TABLE region_dim (
    RegionID INT PRIMARY KEY,
    RegionName VARCHAR(100),
    Country VARCHAR(100),
    Zone VARCHAR(100)
);

-- DIMENSION: store_dim
CREATE TABLE store_dim (
    StoreID INT PRIMARY KEY,
    StoreName VARCHAR(100),
    StoreType VARCHAR(100),
    City VARCHAR(100),
    RegionID INT,
    FOREIGN KEY (RegionID) REFERENCES region_dim(RegionID)
);

-- DIMENSION: product_dim
CREATE TABLE product_dim (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(100),
    Brand VARCHAR(100),
    BasePrice DECIMAL(10, 2)
);

-- DIMENSION: customer_dim (SCD Type 2 Ready)
CREATE TABLE customer_dim (
    CustomerKey INT PRIMARY KEY AUTO_INCREMENT,   -- Surrogate Key
    CustomerID INT NOT NULL,                      
    Email VARCHAR(100),
    Name VARCHAR(100),
    City VARCHAR(100),
    LoyaltyStatus VARCHAR(50),
    IsCurrent CHAR(1),                           
    StartDate DATE,
    EndDate DATE
    
);

CREATE TABLE fact_sales (
    SalesID INT PRIMARY KEY,
    ProductID INT NOT NULL,
    CustomerKey INT NOT NULL,       
    StoreID INT NOT NULL,
    RegionID INT NOT NULL,
    ReturnID INT,                   
    SalesDate DATE NOT NULL,
    UnitsSold INT NOT NULL,
    SalesAmount DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (ProductID) REFERENCES product_dim(ProductID),
    FOREIGN KEY (CustomerKey) REFERENCES customer_dim(CustomerKey),
    FOREIGN KEY (StoreID) REFERENCES store_dim(StoreID),
    FOREIGN KEY (RegionID) REFERENCES region_dim(RegionID)
);

CREATE TABLE fact_returns (
    ReturnID INT PRIMARY KEY,
    SalesID INT NOT NULL,
    ProductID INT NOT NULL,
    CustomerKey INT NOT NULL,
    StoreID INT NOT NULL,
    RegionID INT NOT NULL,
    ReturnDate DATE NOT NULL,
    ReturnAmount DECIMAL(10, 2),
    FOREIGN KEY (SalesID) REFERENCES fact_sales(SalesID),
    FOREIGN KEY (ProductID) REFERENCES product_dim(ProductID),
    FOREIGN KEY (CustomerKey) REFERENCES customer_dim(CustomerKey),
    FOREIGN KEY (StoreID) REFERENCES store_dim(StoreID),
    FOREIGN KEY (RegionID) REFERENCES region_dim(RegionID)
);
