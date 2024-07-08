USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

CREATE OR REPLACE DATABASE FINNHUB_STOCK_MARKET_DATA;

CREATE SCHEMA IF NOT EXISTS FINNHUB_STOCK_MARKET_DATA.raw_data;

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_stockmarket_airflow_dev_bucket_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'your_aws_role_arn'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://stockmarket-airflow-dev-bucket')
;

    /*--
      Execute the command below to retrieve the ARN and External ID for the AWS IAM user that was created automatically for your Snowflake account.
      You’ll use these values to configure permissions for Snowflake in your AWS Management Console:
          https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration#step-5-grant-the-iam-user-permissions-to-access-bucket-objects
    --*/
DESCRIBE INTEGRATION s3_stockmarket_airflow_dev_bucket_integration;
  
SHOW STORAGE INTEGRATIONS;

CREATE OR REPLACE STAGE s3_stockmarket_airflow_dev_bucket_stage
URL = 's3://stockmarket-airflow-dev-bucket'
STORAGE_INTEGRATION = s3_stockmarket_airflow_dev_bucket_integration;

SHOW STAGES;
LIST @s3_stockmarket_airflow_dev_bucket_stage;
SELECT metadata$filename
FROM @s3_stockmarket_airflow_dev_bucket_stage;

CREATE OR REPLACE FILE FORMAT my_json_format
    TYPE = JSON
    COMPRESSION = AUTO
    DATE_FORMAT = AUTO
    TIME_FORMAT = AUTO
    TIMESTAMP_FORMAT = AUTO
    BINARY_FORMAT = HEX
    TRIM_SPACE = FALSE
    NULL_IF = ( '\\N' )
    FILE_EXTENSION = '.json'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = FALSE
    REPLACE_INVALID_CHARACTERS = FALSE
    IGNORE_UTF8_ERRORS = FALSE
    SKIP_BYTE_ORDER_MARK = TRUE 
;

SHOW FILE FORMATS;
-- create a table for copying multiple json files from the stage
CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
(
    file_name VARCHAR(100),
    value VARIANT
);

SHOW TABLES IN FINNHUB_STOCK_MARKET_DATA.public
--create tables to convert the table with variant values into relational data using INSERT INTO 
CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.insider_transactions_aapl
(
    ins_trx_change NUMBER(38,0),
    ins_trx_currency VARCHAR(10),
    ins_trx_filling_date DATE,
    ins_trx_id VARCHAR(200),
    ins_trx_is_derivative BOOLEAN,
    ins_trx_name VARCHAR(100),
    ins_trx_share NUMBER(38,0),
    ins_trx_source VARCHAR(10),
    ins_trx_symbol VARCHAR(10),
    ins_trx_code CHAR(1),
    ins_trx_date DATE,
    ins_trx_price FLOAT 
);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.financials_reported_quarterly 
(
quarter_report_access_number VARCHAR(100),
quarter_report_symbol VARCHAR(10),
quarter_report_cik NUMBER(20,0),
quarter_report_year VARCHAR(10), 
quarter_report_quarter NUMBER(5,0),
quarter_report_form VARCHAR(10),
quarter_report_start_date DATETIME,
quarter_report_end_date DATETIME,
quarter_report_filed_date DATETIME,
quarter_report_accepted_date DATETIME,
quarter_report_concept VARCHAR(100),
quarter_report_unit VARCHAR(10),
quarter_report_label VARCHAR,
quarter_report_value NUMBER(38,0)
);


COPY INTO FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
FROM (
    SELECT metadata$filename,
            t.$1
    FROM @s3_stockmarket_airflow_dev_bucket_stage
         (FILE_FORMAT => 'my_json_format') t);

INSERT INTO FINNHUB_STOCK_MARKET_DATA.raw_data.insider_transactions_aapl 
SELECT
    t.value:change::NUMBER(38,0) AS ins_trx_change,
    t.value:currency::VARCHAR(10) AS ins_trx_currency,
    t.value:filingDate::DATE AS ins_trx_filling_date,
    t.value:id::VARCHAR(200) AS ins_trx_id,
    t.value:isDerivative::BOOLEAN AS ins_trx_is_derivative,
    t.value:name::VARCHAR(100) AS ins_trx_name,
    t.value:share::NUMBER(38,0) AS ins_trx_share,
    t.value:source::VARCHAR(10) AS ins_trx_source,
    t.value:symbol::VARCHAR(10) AS ins_trx_symbol,
    t.value:transactionCode::CHAR(1) AS ins_trx_code,
    t.value:transactionDate::DATE AS ins_trx_date,
    t.value:transactionPrice::FLOAT AS ins_trx_price
FROM (SELECT value FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
WHERE FILE_NAME LIKE '%insider_transactions%') s,
TABLE(FLATTEN(input => s.value:data)) t
;

INSERT INTO FINNHUB_STOCK_MARKET_DATA.raw_data.financials_reported_quarterly
SELECT
    t.value:accessNumber::VARCHAR(100) AS quarter_report_access_number,
    t.value:symbol::VARCHAR(10) AS quarter_report_symbol,
    t.value:cik::NUMBER(20,0) AS quarter_report_cik,
    t.value:year::VARCHAR(10) AS quarter_report_year, 
    t.value:quarter::NUMBER(5,0) AS quarter_report_quarter,
    t.value:form::VARCHAR(10) AS quarter_report_form,
    t.value:startDate::DATETIME AS quarter_report_start_date,
    t.value:endDate::DATETIME AS quarter_report_end_date,
    t.value:filedDate::DATETIME AS quarter_report_filed_date,
    t.value:acceptedDate::DATETIME AS quarter_report_accepted_date,
    r.value:concept::VARCHAR(100) AS quarter_report_concept,
    r.value:unit::VARCHAR(10) AS quarter_report_unit,
    r.value:label::VARCHAR AS quarter_report_label,
    r.value:value::NUMBER(38,0) AS quarter_report_value
FROM (SELECT value 
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%financials_reported_quarterly%') s,
TABLE(FLATTEN(input => s.value:data)) as t,
TABLE(FLATTEN(input => t.value:report.bs)) as r
;

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_metric_aapl
AS (SELECT metric.key::VARCHAR(200) AS key,
           metric.value::VARCHAR AS value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:metric)) as metric);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_gross_margin_aapl
AS (SELECT 
    DISTINCT gross_margin.value:period::DATE AS period,
    gross_margin.value:v::FLOAT as gross_margin_value,
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'grossMargin')) as gross_margin
ORDER BY period DESC);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_operating_margin_aapl
AS (SELECT 
    DISTINCT operating_margin.value:period::DATE AS period,
    operating_margin.value:v::FLOAT as operating_margin_value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'operatingMargin'))as operating_margin
ORDER BY period DESC);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_net_margin_aapl
AS (SELECT 
    DISTINCT net_margin.value:period::DATE AS period,
    net_margin.value:v::FLOAT as net_margin_value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'netMargin'))as net_margin
ORDER BY period DESC);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_fcf_margin_aapl
AS (SELECT 
    DISTINCT fcf_margin.value:period::DATE AS period,
    fcf_margin.value:v::FLOAT as fcf_margin_value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'fcfMargin'))as fcf_margin
ORDER BY period DESC);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_ebitper_share_aapl
AS (SELECT 
    DISTINCT ebit.value:period::DATE AS period,
    ebit.value:v::FLOAT as ebit_per_share_value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'ebitPerShare'))as ebit
ORDER BY period DESC);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_eps_aapl
AS (SELECT 
    DISTINCT eps.value:period::DATE AS period,
    eps.value:v::FLOAT as eps_value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'eps'))as eps
ORDER BY period DESC);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_pretax_margin_aapl
AS (SELECT 
    DISTINCT pretax_margin.value:period::DATE AS period,
    pretax_margin.value:v::FLOAT as pretax_margin_value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'pretaxMargin'))as pretax_margin
ORDER BY period DESC);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_sales_per_share_aapl
AS (SELECT 
    DISTINCT sales_per_share.value:period::DATE AS period,
    sales_per_share.value:v::FLOAT as sales_per_share_value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'salesPerShare'))as sales_per_share
ORDER BY period DESC);

CREATE OR REPLACE TABLE FINNHUB_STOCK_MARKET_DATA.raw_data.basic_financials_sga_to_sale_aapl
AS (SELECT 
    DISTINCT sga_to_sale.value:period::DATE AS period,
    sga_to_sale.value:v::FLOAT as sga_to_sale_value
FROM (SELECT value
      FROM FINNHUB_STOCK_MARKET_DATA.public.copy_multiple_files
      WHERE FILE_NAME LIKE '%basic_financials%') s,
TABLE(FLATTEN(input => s.value:series.annual)) as annual,
TABLE(FLATTEN(input => annual.this, PATH => 'sgaToSale'))as sga_to_sale
ORDER BY period DESC);
_
