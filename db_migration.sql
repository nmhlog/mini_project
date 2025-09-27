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
        sk_employee SERIAL PRIMARY KEY, 
        employee_id INT NOT NULL,
        employee_first_name VARCHAR(100),
        employee_middle_initial VARCHAR(5), 
        employee_last_name VARCHAR(100),
        employee_birth_date date,
        employee_gender VARCHAR(3),
        employee_city_id int,
        employee_city_name VARCHAR(100),
        employee_country_id int ,
        employee_country_code VARCHAR(5),
        employee_country_name VARCHAR(100),
        employee_hire_date	timestamp,
        start_date DATE DEFAULT CURRENT_DATE, 
        end_date DATE DEFAULT '9999-12-31',
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

CREATE TABLE IF NOT EXISTS dm.fact_sales (
    sk_date INTEGER,
    sk_customer INTEGER,
    sk_employee INTEGER,
    sk_product INTEGER,
    sales_id INTEGER unique,
    transaction_no VARCHAR(255),
    quantity DECIMAL(18, 2),
    discount DECIMAL(6, 2),
    total_price DECIMAL(18, 2),
    insert_date DATE DEFAULT CURRENT_DATE
);

    -- Buat stored procedure untuk load dim_product per batch
  CREATE OR REPLACE PROCEDURE dm.sp_load_dim_product(p_batch_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
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
        p_batch_date  -- ← pakai parameter
    FROM stg.products p
    LEFT JOIN stg.categories c 
           ON p.categoryid = c.categoryid
    WHERE p.batchid = p_batch_date  -- ← filter by parameter
    ORDER BY p.productid, p.modifydate DESC
    ON CONFLICT (product_id)
    DO UPDATE SET
        product_name     = EXCLUDED.product_name,
        product_price    = EXCLUDED.product_price,
        category_id      = EXCLUDED.category_id,
        category_name    = EXCLUDED.category_name,
        modify_datetime  = EXCLUDED.modify_datetime,
        insert_date      = EXCLUDED.insert_date;
END;
$$;
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON dm.dim_customer(customer_id) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_stg_customers_batch ON stg.customers(batchid);


CREATE OR REPLACE PROCEDURE dm.sp_load_dim_customer(p_batch_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
		DROP TABLE IF EXISTS customer_batch;
		CREATE TEMPORARY TABLE customer_batch as
        SELECT 
            cu.customerid,
            cu.firstname,
            cu.middleinitial,
            cu.lastname,
            cu.address,
            cu.cityid,
            ci.cityname,
            ci.zipcode,
            co.countryid,
            co.countryname,
            co.countrycode
        FROM stg.customers cu
        LEFT JOIN stg.cities ci 
            ON cu.cityid = ci.cityid 
           AND cu.batchid = ci.batchid
        LEFT JOIN stg.countries co 
            ON ci.countryid = co.countryid 
           AND cu.batchid = co.batchid
        WHERE cu.batchid = p_batch_date;
		
		DROP TABLE IF EXISTS changed_customers;
		CREATE TEMPORARY TABLE changed_customers as
        SELECT cb.*
        FROM customer_batch cb
        JOIN dm.dim_customer d
          ON d.customer_id = cb.customerid
         AND d.is_current = TRUE
        WHERE 
            d.customer_first_name IS DISTINCT FROM cb.firstname OR
            d.customer_middle_initial_name IS DISTINCT FROM cb.middleinitial OR
            d.customer_last_name IS DISTINCT FROM cb.lastname OR
            d.customer_address IS DISTINCT FROM cb.address OR
            d.customer_city_id IS DISTINCT FROM cb.cityid OR
            d.customer_city_name IS DISTINCT FROM cb.cityname OR
            d.customer_zipcode IS DISTINCT FROM cb.zipcode OR
            d.customer_country_id IS DISTINCT FROM cb.countryid OR
            d.customer_country_name IS DISTINCT FROM cb.countryname OR
            d.customer_country_code IS DISTINCT FROM cb.countrycode;
   
    -- 1. Close outdated records
    UPDATE dm.dim_customer d
    SET end_date = p_batch_date,
        is_current = FALSE
    FROM changed_customers s
    WHERE d.customer_id = s.customerid
      AND d.is_current = TRUE;

    -- 2. Insert new/changed records
    INSERT INTO dm.dim_customer (
        customer_id, customer_first_name, customer_middle_initial_name, 
        customer_last_name, customer_address, customer_city_id,
        customer_city_name, customer_zipcode, customer_country_id,
        customer_country_name, customer_country_code,
        start_date, end_date, is_current
    )
    SELECT 
        cb.customerid,
        cb.firstname,
        cb.middleinitial,
        cb.lastname,
        cb.address,
        cb.cityid,
        cb.cityname,
        cb.zipcode,
        cb.countryid,
        cb.countryname,
        cb.countrycode,
        p_batch_date, DATE '9999-12-31', TRUE
    FROM customer_batch cb
    LEFT JOIN dm.dim_customer d
      ON d.customer_id = cb.customerid
     AND d.is_current = TRUE
    WHERE 
        d.customer_id IS NULL -- new customer
        OR (
            d.customer_first_name IS DISTINCT FROM cb.firstname OR
            d.customer_middle_initial_name IS DISTINCT FROM cb.middleinitial OR
            d.customer_last_name IS DISTINCT FROM cb.lastname OR
            d.customer_address IS DISTINCT FROM cb.address OR
            d.customer_city_id IS DISTINCT FROM cb.cityid OR
            d.customer_city_name IS DISTINCT FROM cb.cityname OR
            d.customer_zipcode IS DISTINCT FROM cb.zipcode OR
            d.customer_country_id IS DISTINCT FROM cb.countryid OR
            d.customer_country_name IS DISTINCT FROM cb.countryname OR
            d.customer_country_code IS DISTINCT FROM cb.countrycode
        );
END;
$$;



CREATE OR REPLACE PROCEDURE dm.sp_load_dim_employee(p_batch_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Tutup record lama kalau ada perubahan data
    UPDATE dm.dim_employee d
    SET end_date   = p_batch_date - 1,
        is_current = false
    FROM (
        SELECT 
            e.employeeid,
            e.firstname,
            e.middleinitial,
            e.lastname,
            e.birthdate,
            e.gender,
            e.cityid,
            ci.cityname,
            ci.countryid,
            co.countryname,
            co.countrycode,
            e.hiredate
        FROM stg.employee e
        LEFT JOIN stg.cities ci ON e.cityid = ci.cityid AND e.batchid = ci.batchid
        LEFT JOIN stg.countries co ON ci.countryid = co.countryid AND e.batchid = co.batchid
        WHERE e.batchid = p_batch_date
    ) s
    WHERE d.employee_id = s.employeeid
      AND d.is_current = true   -- hanya record aktif
      AND (
          d.employee_first_name IS DISTINCT FROM s.firstname OR
          d.employee_middle_initial IS DISTINCT FROM s.middleinitial OR
          d.employee_last_name IS DISTINCT FROM s.lastname OR
          d.employee_birth_date IS DISTINCT FROM s.birthdate OR
          d.employee_gender IS DISTINCT FROM s.gender OR
          d.employee_city_id IS DISTINCT FROM s.cityid OR
          d.employee_city_name IS DISTINCT FROM s.cityname OR
          d.employee_country_id IS DISTINCT FROM s.countryid OR
          d.employee_country_name IS DISTINCT FROM s.countryname OR
          d.employee_country_code IS DISTINCT FROM s.countrycode OR
          d.employee_hire_date IS DISTINCT FROM s.hiredate
      );

    -- 2. Insert record baru (untuk data baru atau berubah)
    INSERT INTO dm.dim_employee (
        employee_id, employee_first_name, employee_middle_initial,
        employee_last_name, employee_birth_date, employee_gender,
        employee_city_id, employee_city_name, employee_country_id,
        employee_country_name, employee_country_code, employee_hire_date,
        start_date, end_date, is_current
    )
    SELECT 
        e.employeeid,
        e.firstname,
        e.middleinitial,
        e.lastname,
        e.birthdate,
        e.gender,
        e.cityid,
        ci.cityname,
        ci.countryid,
        co.countryname,
        co.countrycode,
        e.hiredate,
        p_batch_date,           -- start_date = batch date
        DATE '9999-12-31',      -- masih aktif
        true                       -- is_current = true
    FROM stg.employee e
    LEFT JOIN stg.cities ci ON e.cityid = ci.cityid AND e.batchid = ci.batchid
    LEFT JOIN stg.countries co ON ci.countryid = co.countryid AND e.batchid = co.batchid
    WHERE e.batchid = p_batch_date
      AND (
          -- kalau belum ada employee di dim
          NOT EXISTS (
              SELECT 1 FROM dm.dim_employee d WHERE d.employee_id = e.employeeid
          )
          -- atau ada perubahan data dibanding versi aktif
          OR EXISTS (
              SELECT 1 FROM dm.dim_employee d
              WHERE d.employee_id = e.employeeid
                AND d.is_current = true
                AND (
                    d.employee_first_name IS DISTINCT FROM e.firstname OR
                    d.employee_middle_initial IS DISTINCT FROM e.middleinitial OR
                    d.employee_last_name IS DISTINCT FROM e.lastname OR
                    d.employee_birth_date IS DISTINCT FROM e.birthdate OR
                    d.employee_gender IS DISTINCT FROM e.gender OR
                    d.employee_city_id IS DISTINCT FROM e.cityid OR
                    d.employee_city_name IS DISTINCT FROM ci.cityname OR
                    d.employee_country_id IS DISTINCT FROM ci.countryid OR
                    d.employee_country_name IS DISTINCT FROM co.countryname OR
                    d.employee_country_code IS DISTINCT FROM co.countrycode OR
                    d.employee_hire_date IS DISTINCT FROM e.hiredate
                )
          )
      );
END;
$$;

-- drop procedure dm.sp_load_fact_sales_by_batch
-- DROP PROCEDURE dm.sp_load_fact_sales_by_batch(date);

CREATE OR REPLACE PROCEDURE dm.sp_load_fact_sales_by_batch(IN p_batchid date)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Insert new rows for the given batchid
    INSERT INTO dm.fact_sales (
        sk_date,
        sk_customer,
        sk_employee,
        sk_product,
        sales_id,
        transaction_no,
        quantity,
        discount,
        total_price,
        insert_date
    )
     SELECT DISTINCT ON (s.salesid)
        d.sk_date,
        c.sk_customer,
        e.sk_employee,
        p.sk_product,
        s.salesid,
        s.transactionnumber,
        s.quantity,
        s.discount,
quantity*(p.product_price-(p.product_price *discount)) total_price,
        p_batchid
    FROM stg.sales s
    LEFT JOIN dm.dim_time d ON s.salesdate = d.date
    LEFT JOIN dm.dim_customer c ON s.customerid = c.customer_id and c.is_current = true
    LEFT JOIN dm.dim_employee e ON s.salespersonid = e.employee_id and e.is_current = true
    LEFT JOIN dm.dim_product p ON s.productid = p.product_id 
    WHERE s.batchid = p_batchid 
      AND NOT EXISTS (
          SELECT 1
          FROM dm.fact_sales f
          WHERE f.sales_id = s.salesid
      )

    RAISE NOTICE 'Fact table loaded for batchid %', p_batchid;
END;
$procedure$
;

-- fact_sales → dim_time
ALTER TABLE dm.fact_sales
ADD CONSTRAINT fk_fact_sales_time
FOREIGN KEY (sk_date) REFERENCES dm.dim_time(sk_date);

-- fact_sales → dim_customer
ALTER TABLE dm.fact_sales
ADD CONSTRAINT fk_fact_sales_customer
FOREIGN KEY (sk_customer) REFERENCES dm.dim_customer(sk_customer);

-- fact_sales → dim_employee
ALTER TABLE dm.fact_sales
ADD CONSTRAINT fk_fact_sales_employee
FOREIGN KEY (sk_employee) REFERENCES dm.dim_employee(sk_employee);

-- fact_sales → dim_product
ALTER TABLE dm.fact_sales
ADD CONSTRAINT fk_fact_sales_product
FOREIGN KEY (sk_product) REFERENCES dm.dim_product(sk_product);
