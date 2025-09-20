create schema if not exists stg;
create schema if not exists dm;

DO
$$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'stg') LOOP
        EXECUTE 'DROP TABLE IF EXISTS stg.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;

    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'dm') LOOP
        EXECUTE 'DROP TABLE IF EXISTS dm.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END;
$$;

-- Skema: stg

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
    CountryName  VARCHAR(100),
    CountryCode  VARCHAR(10),
    BatchId date
);

-- Tabel: Customers
CREATE TABLE IF NOT EXISTS stg.Customers (
    CustomerID INTEGER,
    FirstName VARCHAR(100),
    MiddleInitial VARCHAR(5),
    LastName VARCHAR(100),
    CityID INTEGER,
    Address TEXT,
    BatchId date
);

-- Tabel: Employee
CREATE TABLE IF NOT EXISTS stg.Employee (
    EmployeeID INTEGER,
    FirstName VARCHAR(100),
    MiddleInitial VARCHAR(100),
    LastName VARCHAR(100),
    BirthDate DATE,
    Gender VARCHAR(10),
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



CREATE TABLE IF NOT EXISTS  dm.dim_product(
sk_product	SERIAL  primary key ,
product_id	INT,
product_name	VARCHAR(50),
product_price	decimal(18,2),
category_id	INT,
category_name	VARCHAR(255),
modify_datetime	timestamp,
insert_date	date
);


create table if not exists dm.dim_customer(
sk_customer	INT,
customer_id	INT,
customer_first_name	VARCHAR(100),
customer_middle_initial_name VARCHAR(100),
customer_last_name	VARCHAR(100),
customer_address	text,
customer_city_id	INT,
customer_city_name	VARCHAR(100),
customer_zipcode	VARCHAR(10),
customer_country_id	INT,
customer_country_name	VARCHAR(100),
customer_country_code	VARCHAR(10),
start_date DATE NOT NULL DEFAULT CURRENT_DATE,
end_date DATE NOT NULL DEFAULT '9999-12-31',
is_current BOOLEAN NOT NULL DEFAULT true

);

create table if not exists dm.dim_employee (
    sk_employee SERIAL PRIMARY KEY,
    employee_id INT NOT NULL,
    employee_first_name TEXT,
    employee_last_name TEXT,
    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    end_date DATE NOT NULL DEFAULT '9999-12-31',
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE table if not exists dm.dim_time (
    sk_date SERIAL PRIMARY KEY, -- SERIAL is an integer that will auto-increment as new rows added
    date DATE,
    days VARCHAR(40),
    month_id INTEGER,
    month_name VARCHAR(40),
    year INTEGER
);

truncate table dm.dim_time;
INSERT INTO dm.dim_time(date, days, month_id, month_name, year)
SELECT 
days.d::DATE as date, 
to_char(days.d, 'FMMonth DD, YYYY') as days, 
to_char(days.d, 'MM')::integer as month_id, 
to_char(days.d, 'FMMonth') as month_name, 
to_char(days.d, 'YYYY')::integer as year
from (
    SELECT generate_series(
        ('2000-01-01')::date, -- 'start' date
        ('2100-12-31')::date, -- 'end' date
        interval '1 day'  -- one for each day between the start and day
        )) as days(d);