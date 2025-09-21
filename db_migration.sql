    CREATE SCHEMA IF NOT EXISTS stg;
    CREATE SCHEMA IF NOT EXISTS dm;

    DO $$ DECLARE r RECORD; BEGIN FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'stg') LOOP EXECUTE 'DROP TABLE IF EXISTS stg.' || quote_ident(r.tablename) || ' CASCADE'; END LOOP; FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'dm') LOOP EXECUTE 'DROP TABLE IF EXISTS dm.' || quote_ident(r.tablename) || ' CASCADE'; END LOOP; END; $$;

    -- STG Tables
    CREATE TABLE IF NOT EXISTS stg.sales (
        salesid INT, salespersonid INT, customerid INT, productid INT,
        quantity INT, discount NUMERIC(6,2), totalprice NUMERIC(18,2),
        salesdate DATE, transactionnumber VARCHAR(255), batchid DATE
    );

    CREATE TABLE IF NOT EXISTS stg.categories (
        categoryid INT, categoryname VARCHAR(255), batchid DATE
    );

    CREATE TABLE IF NOT EXISTS stg.cities (
        cityid INT, cityname VARCHAR(255), zipcode VARCHAR(10),
        countryid INT, batchid DATE
    );

    CREATE TABLE IF NOT EXISTS stg.countries (
        countryid INT, countryname VARCHAR(100), countrycode VARCHAR(10), batchid DATE
    );

    CREATE TABLE IF NOT EXISTS stg.customers (
        customerid INT, firstname VARCHAR(100), middleinitial VARCHAR(5),
        lastname VARCHAR(100), cityid INT, address TEXT, batchid DATE
    );

    CREATE TABLE IF NOT EXISTS stg.employee (
        employeeid INT, firstname VARCHAR(100), middleinitial VARCHAR(100),
        lastname VARCHAR(100), birthdate DATE, gender VARCHAR(10),
        cityid INT, hiredate TIMESTAMP, batchid DATE
    );

    CREATE TABLE IF NOT EXISTS stg.products (
        productid INT, productname VARCHAR(50), price NUMERIC(18,2),
        categoryid INT, class VARCHAR(25), modifydate TIMESTAMP,
        resistant VARCHAR(25), isallergic BOOLEAN, vitalitydays VARCHAR(10),
        batchid DATE
    );

    -- DM Tables
    CREATE TABLE IF NOT EXISTS dm.dim_product (
        sk_product SERIAL PRIMARY KEY, product_id INT, product_name VARCHAR(50),
        product_price NUMERIC(18,2), category_id INT, category_name VARCHAR(255),
        modify_datetime TIMESTAMP, insert_date DATE
    );

    CREATE TABLE IF NOT EXISTS dm.dim_customer (
        sk_customer SERIAL PRIMARY KEY, customer_id INT, customer_first_name VARCHAR(100),
        customer_middle_initial_name VARCHAR(100), customer_last_name VARCHAR(100),
        customer_address TEXT, customer_city_id INT, customer_city_name VARCHAR(100),
        customer_zipcode VARCHAR(10), customer_country_id INT,
        customer_country_name VARCHAR(100), customer_country_code VARCHAR(10),
        start_date DATE DEFAULT CURRENT_DATE, end_date DATE DEFAULT '9999-12-31',
        is_current BOOLEAN DEFAULT TRUE
    );

    CREATE TABLE IF NOT EXISTS dm.dim_employee (
        sk_employee SERIAL PRIMARY KEY, employee_id INT NOT NULL,
        employee_first_name VARCHAR(100), employee_last_name VARCHAR(100),
        start_date DATE DEFAULT CURRENT_DATE, end_date DATE DEFAULT '9999-12-31',
        is_current BOOLEAN DEFAULT TRUE
    );

    CREATE TABLE IF NOT EXISTS dm.dim_time (
        sk_date SERIAL PRIMARY KEY, date DATE, days VARCHAR(40),
        month_id INT, month_name VARCHAR(40), year INT
    );

    TRUNCATE TABLE dm.dim_time;

    INSERT INTO dm.dim_time(date, days, month_id, month_name, year)
    SELECT d::DATE, to_char(d, 'FMMonth DD, YYYY'), to_char(d,'MM')::INT,
        to_char(d,'FMMonth'), to_char(d,'YYYY')::INT
    FROM generate_series('2000-01-01'::DATE, '2100-12-31'::DATE, interval '1 day') AS d;


    CREATE UNIQUE INDEX idx_product_id ON dm.dim_product(product_id);


    -- Buat stored procedure untuk load dim_product per batch
CREATE OR REPLACE PROCEDURE dm.load_dim_product()
LANGUAGE plpgsql
AS $$
DECLARE
    v_batchid date;
BEGIN

    FOR v_batchid IN 
        SELECT DISTINCT batchid FROM stg.products ORDER BY batchid ASC
    LOOP
        RAISE NOTICE 'Processing BATCH ID = %', v_batchid;

        INSERT INTO dm.dim_product (
            product_id, product_name, product_price, category_id,
            category_name, modify_datetime, insert_date
        )
        SELECT DISTINCT ON (p.productid)
            p.productid,
            p.productname,
            p.price,
            p.categoryid,
            c.categoryname,
            p.modifydate,
            v_batchid
        FROM stg.products p
        LEFT JOIN stg.categories c ON p.categoryid = c.categoryid
        WHERE p.batchid = v_batchid
        ORDER BY p.productid, p.modifydate DESC
        ON CONFLICT (product_id)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            product_price = EXCLUDED.product_price,
            category_id = EXCLUDED.category_id,
            category_name = EXCLUDED.category_name,
            modify_datetime = EXCLUDED.modify_datetime,
            insert_date = EXCLUDED.insert_date;
    END LOOP;
END;
$$;
