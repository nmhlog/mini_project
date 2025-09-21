GET_LIST_PROCESS_BATCH = "



"

DIM_CUSTOMER  = """
    INSERT INTO dm.dim_customer (
    customer_id, customer_first_name, customer_middle_initial_name, 
    customer_last_name, customer_address, customer_city_id,
    customer_city_name, customer_zipcode, customer_country_id,
    customer_country_name, customer_country_code, start_date, end_date,is_current
)
SELECT 
    cu.customerid,
    cu.firstname,
    cu.middleinitial,
    cu.lastname,
    cu.address,
    cu.cityid,
    ci.cityname,
    ci.zipcode,
    ci.countryid,
    co.countryname,
    co.countrycode,
    CURRENT_DATE,
    '9999-12-31',
    1
FROM stg.customers cu
LEFT JOIN stg.cities ci ON cu.cityid = ci.cityid AND ci.batchid = cu.batchid
LEFT JOIN stg.countries co ON ci.countryid = co.countryid and co.batchid = ci.batchid
WHERE  cu.BatchId = :batch_id
ON CONFLICT (customer_id)
DO UPDATE SET
    customer_first_name = EXCLUDED.customer_first_name,
    customer_middle_initial_name = EXCLUDED.customer_middle_initial_name,
    customer_last_name = EXCLUDED.customer_last_name,
    customer_address = EXCLUDED.customer_address,
    customer_city_id = EXCLUDED.customer_city_id,
    customer_city_name = EXCLUDED.customer_city_name,
    customer_zipcode = EXCLUDED.customer_zipcode,
    customer_country_id = EXCLUDED.customer_country_id,
    customer_country_name = EXCLUDED.customer_country_name,
    customer_country_code = EXCLUDED.customer_country_code,
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date;

    
    """