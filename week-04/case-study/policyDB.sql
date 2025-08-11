--- metadata table to hold all table information

CREATE TABLE MetadataDataFlowConfig (
    TableName VARCHAR(100) NOT NULL,
    SourceQuery VARCHAR(MAX) NOT NULL,
    SinkPath VARCHAR(500) NOT NULL,
);

select * from MetadataDataFlowConfig;

INSERT INTO MetadataDataFlowConfig (TableName, SourceQuery, SinkPath) VALUES
('Policy', 'SELECT * FROM dbo.Policy', '/raw/bronze/policy/'),
('PolicyHolder', 'SELECT * FROM dbo.PolicyHolder', '/raw/bronze/policyholder/'),
('Coverage', 'SELECT * FROM dbo.Coverage', '/raw/bronze/coverage/'),
('Premium', 'SELECT * FROM dbo.Premium', '/raw/bronze/premium/'),
('Claims', 'SELECT * FROM dbo.Claims', '/raw/bronze/claims/'),
('Beneficiaries', 'SELECT * FROM dbo.Beneficiaries', '/raw/bronze/beneficiaries/'),
('Agents', 'SELECT * FROM dbo.Agents', '/raw/bronze/agents/'),
('PolicyDocuments', 'SELECT * FROM dbo.PolicyDocuments', '/raw/bronze/policydocuments/'),
('PolicyStatus', 'SELECT * FROM dbo.PolicyStatus', '/raw/bronze/policystatus/'),
('Payments', 'SELECT * FROM dbo.Payments', '/raw/bronze/payments/');

--- table creation of policyDB
CREATE TABLE PolicyHolder (
    PolicyHolderID INT IDENTITY(1,1) PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DateOfBirth DATE,
    Email VARCHAR(100),
    Phone VARCHAR(20),
    Address VARCHAR(200)
    );


CREATE TABLE PolicyStatus (
    StatusID INT IDENTITY(1,1) PRIMARY KEY,
    StatusName VARCHAR(50) NOT NULL
);

CREATE TABLE Policy (
    PolicyID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyNumber VARCHAR(50) NOT NULL,
    PolicyType VARCHAR(50),
    StartDate DATE,
    EndDate DATE,
    PolicyHolderID INT NOT NULL,
    StatusID INT NOT NULL,
    FOREIGN KEY (PolicyHolderID) REFERENCES PolicyHolder(PolicyHolderID),
    FOREIGN KEY (StatusID) REFERENCES PolicyStatus(StatusID)
);


CREATE TABLE Coverage (
    CoverageID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL,
    CoverageType VARCHAR(50),
    CoverageAmount DECIMAL(18,2),
    Deductible DECIMAL(18,2),
    FOREIGN KEY (PolicyID) REFERENCES Policy(PolicyID)
);

CREATE TABLE Premium (
    PremiumID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL,
    Amount DECIMAL(18,2),
    DueDate DATE,
    PaymentFrequency VARCHAR(20),
    FOREIGN KEY (PolicyID) REFERENCES Policy(PolicyID)
);

CREATE TABLE Claims (
    ClaimID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL,
    ClaimDate DATE,
    ClaimAmount DECIMAL(18,2),
    ClaimStatus VARCHAR(50),
    FOREIGN KEY (PolicyID) REFERENCES Policy(PolicyID)
);

CREATE TABLE Beneficiaries (
    BeneficiaryID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Relationship VARCHAR(50),
    FOREIGN KEY (PolicyID) REFERENCES Policy(PolicyID)
);

CREATE TABLE Agents (
    AgentID INT IDENTITY(1,1) PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    Phone VARCHAR(20)
);

CREATE TABLE PolicyDocuments (
    DocumentID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL,
    DocumentName VARCHAR(100),
    DocumentType VARCHAR(50),
    DocumentURL VARCHAR(500),
    UploadDate DATE,
    FOREIGN KEY (PolicyID) REFERENCES Policy(PolicyID)
);


CREATE TABLE Payments (
    PaymentID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL,
    PaymentDate DATE,
    Amount DECIMAL(18,2),
    PaymentMethod VARCHAR(50),
    FOREIGN KEY (PolicyID) REFERENCES Policy(PolicyID)
);

--- insert data into tables

INSERT INTO PolicyHolder (FirstName, LastName, DateOfBirth, Email, Phone, Address) VALUES
('John', 'Doe', '1980-01-15', 'john.doe@example.com', '555-1234', '123 Elm St'),
('Jane', 'Smith', '1975-03-22', 'jane.smith@example.com', '555-5678', '456 Oak St'),
('Michael', 'Johnson', '1990-07-10', 'michael.j@example.com', '555-8765', '789 Pine St'),
('Emily', 'Davis', '1985-05-30', 'emily.davis@example.com', '555-4321', '321 Maple Ave'),
('David', 'Wilson', '1970-12-11', 'david.w@example.com', '555-6543', '654 Cedar Rd'),
('Linda', 'Brown', '1982-08-20', 'linda.brown@example.com', '555-3456', '987 Spruce St'),
('Robert', 'Jones', '1965-09-14', 'robert.j@example.com', '555-7890', '159 Birch Ln'),
('Patricia', 'Miller', '1995-04-25', 'patricia.m@example.com', '555-0987', '753 Walnut St'),
('James', 'Taylor', '1988-11-01', 'james.t@example.com', '555-2468', '852 Chestnut St'),
('Barbara', 'Anderson', '1978-06-06', 'barbara.a@example.com', '555-1357', '951 Poplar Rd'),
('Charles', 'Thomas', '1983-02-17', 'charles.t@example.com', '555-9753', '147 Willow Ave'),
('Jennifer', 'Jackson', '1992-10-23', 'jennifer.j@example.com', '555-8642', '258 Aspen St'),
('Joseph', 'White', '1969-07-07', 'joseph.w@example.com', '555-7531', '369 Dogwood Ln'),
('Susan', 'Harris', '1981-03-03', 'susan.h@example.com', '555-6420', '741 Magnolia Dr'),
('Thomas', 'Martin', '1977-09-29', 'thomas.m@example.com', '555-5319', '852 Hickory St'),
('Sarah', 'Thompson', '1984-12-18', 'sarah.t@example.com', '555-4208', '963 Cottonwood Rd'),
('Christopher', 'Garcia', '1993-05-12', 'chris.g@example.com', '555-3197', '159 Cypress St'),
('Karen', 'Martinez', '1976-08-28', 'karen.m@example.com', '555-2086', '357 Redwood Ave'),
('Daniel', 'Robinson', '1987-04-16', 'daniel.r@example.com', '555-1975', '456 Sycamore Rd'),
('Nancy', 'Clark', '1991-11-09', 'nancy.c@example.com', '555-0864', '654 Fir St');



INSERT INTO PolicyStatus (StatusName) VALUES
('Active'),
('Cancelled'),
('Lapsed'),
('Pending'),
('Expired'),
('Under Review'),
('Suspended'),
('Renewed'),
('Terminated'),
('Claimed'),
('In Force'),
('Inactive'),
('Paid Up'),
('Non-Payment'),
('Reinstated'),
('Draft'),
('Submitted'),
('Approved'),
('Declined'),
('Closed');

INSERT INTO Policy (PolicyNumber, PolicyType, StartDate, EndDate, PolicyHolderID, StatusID) VALUES
('P1001', 'Auto', '2023-01-01', '2024-01-01', 1, 1),
('P1002', 'Home', '2023-02-01', '2024-02-01', 2, 1),
('P1003', 'Life', '2023-03-01', '2043-03-01', 3, 1),
('P1004', 'Health', '2023-04-01', '2024-04-01', 4, 4),
('P1005', 'Auto', '2023-05-01', '2024-05-01', 5, 1),
('P1006', 'Home', '2023-06-01', '2024-06-01', 6, 2),
('P1007', 'Life', '2023-07-01', '2043-07-01', 7, 1),
('P1008', 'Health', '2023-08-01', '2024-08-01', 8, 3),
('P1009', 'Auto', '2023-09-01', '2024-09-01', 9, 1),
('P1010', 'Home', '2023-10-01', '2024-10-01', 10, 1),
('P1011', 'Life', '2023-11-01', '2043-11-01', 11, 1),
('P1012', 'Health', '2023-12-01', '2024-12-01', 12, 1),
('P1013', 'Auto', '2024-01-01', '2025-01-01', 13, 1),
('P1014', 'Home', '2024-02-01', '2025-02-01', 14, 1),
('P1015', 'Life', '2024-03-01', '2044-03-01', 15, 1),
('P1016', 'Health', '2024-04-01', '2025-04-01', 16, 1),
('P1017', 'Auto', '2024-05-01', '2025-05-01', 17, 1),
('P1018', 'Home', '2024-06-01', '2025-06-01', 18, 1),
('P1019', 'Life', '2024-07-01', '2044-07-01', 19, 1),
('P1020', 'Health', '2024-08-01', '2025-08-01', 20, 1);


INSERT INTO Coverage (PolicyID, CoverageType, CoverageAmount, Deductible) VALUES
(1, 'Liability', 500000, 1000),
(2, 'Fire', 300000, 500),
(3, 'Life', 1000000, 0),
(4, 'Medical', 200000, 250),
(5, 'Collision', 250000, 1500),
(6, 'Theft', 100000, 750),
(7, 'Life', 1500000, 0),
(8, 'Medical', 300000, 300),
(9, 'Liability', 600000, 1200),
(10, 'Fire', 350000, 600),
(11, 'Life', 2000000, 0),
(12, 'Medical', 250000, 275),
(13, 'Collision', 300000, 1400),
(14, 'Theft', 120000, 700),
(15, 'Life', 1800000, 0),
(16, 'Medical', 220000, 260),
(17, 'Liability', 550000, 1100),
(18, 'Fire', 320000, 550),
(19, 'Life', 1600000, 0),
(20, 'Medical', 210000, 250);


INSERT INTO Premium (PolicyID, Amount, DueDate, PaymentFrequency) VALUES
(1, 1200.00, '2023-12-31', 'Annually'),
(2, 900.00, '2023-12-15', 'Annually'),
(3, 1500.00, '2023-12-01', 'Annually'),
(4, 800.00, '2023-11-30', 'Annually'),
(5, 1100.00, '2023-12-25', 'Annually'),
(6, 950.00, '2023-12-20', 'Annually'),
(7, 1800.00, '2023-12-05', 'Annually'),
(8, 850.00, '2023-12-10', 'Annually'),
(9, 1300.00, '2023-12-18', 'Annually'),
(10, 900.00, '2023-12-22', 'Annually'),
(11, 1900.00, '2023-12-07', 'Annually'),
(12, 870.00, '2023-12-11', 'Annually'),
(13, 1250.00, '2024-01-15', 'Annually'),
(14, 920.00, '2024-01-20', 'Annually'),
(15, 1750.00, '2024-02-01', 'Annually'),
(16, 880.00, '2024-02-10', 'Annually'),
(17, 1150.00, '2024-02-15', 'Annually'),
(18, 940.00, '2024-02-18', 'Annually'),
(19, 1600.00, '2024-03-01', 'Annually'),
(20, 860.00, '2024-03-05', 'Annually');


INSERT INTO Claims (PolicyID, ClaimDate, ClaimAmount, ClaimStatus) VALUES
(1, '2023-06-15', 2500.00, 'Closed'),
(2, '2023-07-10', 1500.00, 'Open'),
(3, '2023-05-20', 10000.00, 'Closed'),
(4, '2023-08-01', 5000.00, 'Pending'),
(5, '2023-04-10', 2000.00, 'Rejected'),
(6, '2023-07-25', 3000.00, 'Closed'),
(7, '2023-06-05', 12000.00, 'Open'),
(8, '2023-08-15', 7000.00, 'Pending'),
(9, '2023-05-30', 3500.00, 'Closed'),
(10, '2023-06-20', 4000.00, 'Rejected'),
(11, '2023-07-15', 11000.00, 'Open'),
(12, '2023-05-10', 6000.00, 'Closed'),
(13, '2023-06-25', 2700.00, 'Pending'),
(14, '2023-07-05', 3100.00, 'Closed'),
(15, '2023-08-10', 15000.00, 'Open'),
(16, '2023-04-25', 4500.00, 'Rejected'),
(17, '2023-07-20', 5000.00, 'Closed'),
(18, '2023-05-05', 3200.00, 'Open'),
(19, '2023-06-30', 9000.00, 'Closed'),
(20, '2023-08-05', 4000.00, 'Pending');

INSERT INTO Beneficiaries (PolicyID, FirstName, LastName, Relationship) VALUES
(1, 'Alice', 'Doe', 'Spouse'),
(2, 'Bob', 'Smith', 'Child'),
(3, 'Carol', 'Johnson', 'Spouse'),
(4, 'Dan', 'Davis', 'Parent'),
(5, 'Eve', 'Wilson', 'Child'),
(6, 'Frank', 'Brown', 'Spouse'),
(7, 'Grace', 'Jones', 'Child'),
(8, 'Hank', 'Miller', 'Spouse'),
(9, 'Ivy', 'Taylor', 'Child'),
(10, 'Jack', 'Anderson', 'Parent'),
(11, 'Kate', 'Thomas', 'Spouse'),
(12, 'Leo', 'Jackson', 'Child'),
(13, 'Mia', 'White', 'Spouse'),
(14, 'Ned', 'Harris', 'Child'),
(15, 'Olivia', 'Martin', 'Parent'),
(16, 'Paul', 'Thompson', 'Spouse'),
(17, 'Quinn', 'Garcia', 'Child'),
(18, 'Rose', 'Martinez', 'Spouse'),
(19, 'Steve', 'Robinson', 'Child'),
(20, 'Tina', 'Clark', 'Parent');

INSERT INTO Agents (FirstName, LastName, Email, Phone) VALUES
('Agent', 'One', 'agent.one@example.com', '555-1001'),
('Agent', 'Two', 'agent.two@example.com', '555-1002'),
('Agent', 'Three', 'agent.three@example.com', '555-1003'),
('Agent', 'Four', 'agent.four@example.com', '555-1004'),
('Agent', 'Five', 'agent.five@example.com', '555-1005'),
('Agent', 'Six', 'agent.six@example.com', '555-1006'),
('Agent', 'Seven', 'agent.seven@example.com', '555-1007'),
('Agent', 'Eight', 'agent.eight@example.com', '555-1008'),
('Agent', 'Nine', 'agent.nine@example.com', '555-1009'),
('Agent', 'Ten', 'agent.ten@example.com', '555-1010'),
('Agent', 'Eleven', 'agent.eleven@example.com', '555-1011'),
('Agent', 'Twelve', 'agent.twelve@example.com', '555-1012'),
('Agent', 'Thirteen', 'agent.thirteen@example.com', '555-1013'),
('Agent', 'Fourteen', 'agent.fourteen@example.com', '555-1014'),
('Agent', 'Fifteen', 'agent.fifteen@example.com', '555-1015'),
('Agent', 'Sixteen', 'agent.sixteen@example.com', '555-1016'),
('Agent', 'Seventeen', 'agent.seventeen@example.com', '555-1017'),
('Agent', 'Eighteen', 'agent.eighteen@example.com', '555-1018'),
('Agent', 'Nineteen', 'agent.nineteen@example.com', '555-1019'),
('Agent', 'Twenty', 'agent.twenty@example.com', '555-1020');


INSERT INTO PolicyDocuments (PolicyID, DocumentName, DocumentType, DocumentURL, UploadDate) VALUES
(1, 'PolicyDoc1.pdf', 'PDF', 'http://example.com/docs/policy1.pdf', '2023-01-01'),
(2, 'PolicyDoc2.pdf', 'PDF', 'http://example.com/docs/policy2.pdf', '2023-02-01'),
(3, 'PolicyDoc3.pdf', 'PDF', 'http://example.com/docs/policy3.pdf', '2023-03-01'),
(4, 'PolicyDoc4.pdf', 'PDF', 'http://example.com/docs/policy4.pdf', '2023-04-01'),
(5, 'PolicyDoc5.pdf', 'PDF', 'http://example.com/docs/policy5.pdf', '2023-05-01'),
(6, 'PolicyDoc6.pdf', 'PDF', 'http://example.com/docs/policy6.pdf', '2023-06-01'),
(7, 'PolicyDoc7.pdf', 'PDF', 'http://example.com/docs/policy7.pdf', '2023-07-01'),
(8, 'PolicyDoc8.pdf', 'PDF', 'http://example.com/docs/policy8.pdf', '2023-08-01'),
(9, 'PolicyDoc9.pdf', 'PDF', 'http://example.com/docs/policy9.pdf', '2023-09-01'),
(10, 'PolicyDoc10.pdf', 'PDF', 'http://example.com/docs/policy10.pdf', '2023-10-01'),
(11, 'PolicyDoc11.pdf', 'PDF', 'http://example.com/docs/policy11.pdf', '2023-11-01'),
(12, 'PolicyDoc12.pdf', 'PDF', 'http://example.com/docs/policy12.pdf', '2023-12-01'),
(13, 'PolicyDoc13.pdf', 'PDF', 'http://example.com/docs/policy13.pdf', '2024-01-01'),
(14, 'PolicyDoc14.pdf', 'PDF', 'http://example.com/docs/policy14.pdf', '2024-02-01'),
(15, 'PolicyDoc15.pdf', 'PDF', 'http://example.com/docs/policy15.pdf', '2024-03-01'),
(16, 'PolicyDoc16.pdf', 'PDF', 'http://example.com/docs/policy16.pdf', '2024-04-01'),
(17, 'PolicyDoc17.pdf', 'PDF', 'http://example.com/docs/policy17.pdf', '2024-05-01'),
(18, 'PolicyDoc18.pdf', 'PDF', 'http://example.com/docs/policy18.pdf', '2024-06-01'),
(19, 'PolicyDoc19.pdf', 'PDF', 'http://example.com/docs/policy19.pdf', '2024-07-01'),
(20, 'PolicyDoc20.pdf', 'PDF', 'http://example.com/docs/policy20.pdf', '2024-08-01');


INSERT INTO Payments (PolicyID, PaymentDate, Amount, PaymentMethod) VALUES
(1, '2023-01-15', 1200.00, 'Credit Card'),
(2, '2023-02-15', 900.00, 'Bank Transfer'),
(3, '2023-03-15', 1500.00, 'Credit Card'),
(4, '2023-04-15', 800.00, 'Cash'),
(5, '2023-05-15', 1100.00, 'Credit Card'),
(6, '2023-06-15', 950.00, 'Bank Transfer'),
(7, '2023-07-15', 1800.00, 'Credit Card'),
(8, '2023-08-15', 850.00, 'Cash'),
(9, '2023-09-15', 1300.00, 'Credit Card'),
(10, '2023-10-15', 900.00, 'Bank Transfer'),
(11, '2023-11-15', 1900.00, 'Credit Card'),
(12, '2023-12-15', 870.00, 'Cash'),
(13, '2024-01-15', 1250.00, 'Credit Card'),
(14, '2024-02-15', 920.00, 'Bank Transfer'),
(15, '2024-03-15', 1750.00, 'Credit Card'),
(16, '2024-04-15', 880.00, 'Cash'),
(17, '2024-05-15', 1150.00, 'Credit Card'),
(18, '2024-06-15', 940.00, 'Bank Transfer'),
(19, '2024-07-15', 1600.00, 'Credit Card'),
(20, '2024-08-15', 860.00, 'Cash');

select * from Payments;






