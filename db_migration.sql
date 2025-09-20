
DROP TABLE IF EXISTS stg.Sales;

DROP TABLE IF EXISTS stg.Categories;

DROP TABLE IF EXISTS stg.Cities ;

DROP TABLE IF EXISTS stg.Countries;

DROP TABLE IF EXISTS stg.Customers ;

DROP TABLE IF EXISTS stg.Employee ;

DROP TABLE IF EXISTS stg.Employee ;
DROP TABLE IF EXISTS stg.Products ;
-- Skema: stg
create schema if not exists stg;
-- Tabel: Sales
CREATE TABLE IF NOT EXISTS stg.Sales (
    SalesID INTEGER,
    SalesPersonID INTEGER,
    CustomerID INTEGER,
    ProductID INTEGER,
    Quantity INTEGER,
    Discount NUMERIC(6,2),
    TotalPrice NUMERIC(18,2),
    SalesDate DATE,
    TransactionNumber VARCHAR(255),
    BatchId date
);

-- Tabel: Categories
CREATE TABLE IF NOT EXISTS stg.Categories (
    CategoryID INTEGER,
    CategoryName VARCHAR(255),
    BatchId date
);

-- Tabel: Cities
CREATE TABLE IF NOT EXISTS stg.Cities (
    CityID INTEGER,
    CityName  VARCHAR(255),
    Zipcode  VARCHAR(10),
    CountryID INTEGER,        
    BatchId date
);

-- Tabel: Countries
CREATE TABLE IF NOT EXISTS stg.Countries (
    CountryID INTEGER,
    CountryName  VARCHAR(255),
    CountryCode  VARCHAR(25),
    BatchId date
);

-- Tabel: Customers
CREATE TABLE IF NOT EXISTS stg.Customers (
    CustomerID INTEGER,
    FirstName VARCHAR(255),
    MiddleInitial VARCHAR(5),
    LastName VARCHAR(255),
    CityID INTEGER,
    Address TEXT,
    BatchId date
);

-- Tabel: Employee
CREATE TABLE IF NOT EXISTS stg.Employee (
    EmployeeID INTEGER,
    FirstName TEXT,
    MiddleInitial TEXT,
    LastName TEXT,
    BirthDate DATE,
    Gender TEXT,
    CityID INTEGER,
    HireDate TIMESTAMP,
    BatchId date
);

-- Tabel: Products
CREATE TABLE IF NOT EXISTS stg.Products (
    ProductID INTEGER,
    ProductName VARCHAR(50),
    Price NUMERIC(18,2),
    CategoryID INTEGER,
    Class VARCHAR(25),
    ModifyDate TIMESTAMP,
    Resistant VARCHAR(25),
    IsAllergic boolean,
    VitalityDays VARCHAR(10),
    BatchId date
);
