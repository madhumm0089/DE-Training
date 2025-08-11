create table metadataConf(
TableName Nvarchar(200),
sourceQuery Nvarchar(max),
sinkPath Nvarchar(200),
dqRules Nvarchar(max)
);

drop table metadataConf;

select * from metadataConf;

insert into metadataConf(TableName, sourceQuery, sinkPath, dqRules)
values
('Customer', 'SELECT * FROM dbo.Customer', 'raw/bronze/Customer/', 'Name!=null;Email!=null'),
('Transactions', 'SELECT * FROM dbo.Transactions', 'raw/bronze/Transactions/', 'Amount>0;TransactionDate!=null'),
('Products', 'SELECT * FROM dbo.Products', 'raw/bronze/Products/', 'Price>0;ProductName!=null');

create table Customer(
