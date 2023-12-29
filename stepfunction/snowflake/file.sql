-- Tạo Database và chọn Database và Schema
CREATE DATABASE IF NOT EXISTS manhzeff;
USE DATABASE manhzeff;
CREATE SCHEMA IF NOT EXISTS test;
USE SCHEMA test;

desc INTEGRATION my_s3_integrations

-- Tạo Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integrations
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::902931391129:role/snowflake_integration'
STORAGE_ALLOWED_LOCATIONS = ('s3://dataengineer123/haha/');


-- Tạo Stage
CREATE OR REPLACE STAGE ir_configuration_stage_test
URL='s3://dataengineer123/haha/'
FILE_FORMAT =(TYPE = "json")
STORAGE_INTEGRATION = my_s3_integrations;


CREATE OR REPLACE TABLE staging (
    id INT PRIMARY KEY,
    name STRING(255) NOT NULL,
    value STRING(255),
    hash_pk STRING(255) NOT NULL,
    hash STRING(255) NOT NULL
    extract_time TIMESTAMP NOT NULL
);

CREATE OR REPLACE TABLE source (
    id INT PRIMARY KEY,
    name STRING,
    value STRING,
    extract_time TIMESTAMP_NTZ(9)
);

-- Inserting demo data into the staging table
INSERT INTO staging (id, name, value, hash_pk, hash, extract_time)
VALUES
(1, 'Alice', 'Value1', hash(1), 'hashA', '2023-10-25 08:00:00'),
(2, 'Bob', 'Value2', hash2, 'hashB', '2023-10-25 08:05:00'),
(3, 'Charlie', 'Value3', 'hash3', 'hashC', '2023-10-25 08:10:00'),
(4, 'Dave', 'Value4', 'hash4', 'hashD', '2023-10-25 08:15:00'),
(5, 'Eve', 'Value5', 'hash5', 'hashE', '2023-10-25 08:20:00');

INSERT INTO source (id, name, value, extract_time)
VALUES
(1, 'Alice', 'Value1', '2023-10-25 08:00:00'),
(2, 'Bob', 'Value2', '2023-10-25 08:05:00'),
(3, 'Charlie', 'Value3', '2023-10-25 08:10:00'),
(4, 'Dave', 'Value4', '2023-10-25 08:15:00'),
(5, 'Eve', 'Value5', '2023-10-25 08:20:00');

-- Checking the inserted data
SELECT * FROM source; 
SELECT * FROM staging;

insert into staging  
select  id, name,value,hash(id),hash(name,value), extract_time
from source;


truncate table historical;
truncate table source;
truncate table staging;

select * from  historical;

drop stage MANHZEFF.TEST.IR_CONFIGURATION_STAGE_TEST;

drop stream MANHZEFF.TEST."historical_stream";

-- Tạo Historical Table
CREATE or replace TABLE historical (
    id INT,
    name STRING,
    value STRING,
    hash_pk STRING,
    hash STRING,
    extract_time TIMESTAMP,
    action STRING
);


-- Tạo Latest Table
CREATE or REPLACE TABLE latest (
    id INT,
    name STRING,
    value STRING,
    hash_pk STRING,
    hash STRING,
    extract_time TIMESTAMP
);

ALTER PIPE staging_pipe REFRESH;

SELECT * FROM staging;

COPY INTO staging
FROM @ir_configuration_stage_test
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

drop pipe "my_pipe";
drop pipe staging_pipe;

-- Tạo Stream trên bảng Staging
CREATE OR REPLACE STREAM staging_stream ON TABLE staging;

CREATE OR REPLACE PIPE staging_pipe AUTO_INGEST = TRUE AS 
COPY INTO staging
FROM @ir_configuration_stage_test
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

describe pipe "my_pipe"
-- Tạo Warehouse
CREATE WAREHOUSE IF NOT EXISTS kaigishitsu
WAREHOUSE_SIZE = 'SMALL'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;


-- Tạo Task thực hiện toàn bộ logic SCD Type 6
CREATE OR REPLACE TASK scd_type_6_task
WAREHOUSE = 'kaigishitsu'
SCHEDULE = '1 MINUTE'

ALLOW_OVERLAPPING_EXECUTION = FALSE
AS 

INSERT INTO
    historical 
SELECT
    *
FROM
    (
        -- recorded in new snapshot execution
        SELECT
            *,
            'RECORD' AS action
        FROM
            staging C
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    (
                        select
                            *
                        from
                            historical 
                        QUALIFY RANK() OVER(
                                PARTITION BY hash_pk
                                ORDER BY
                                    extract_time desc
                            ) = 1
                    ) P
                WHERE
                    P.hash_pk = C.hash_pk
                    AND P.hash = C.hash
            )
        UNION
        ALL -- Query records which are updated in new snapshot
        SELECT
            *,
            'UPDATED' AS action
        FROM
            staging C
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    (
                        select
                            *
                        from
                            historical 
                        QUALIFY RANK() OVER(
                                PARTITION BY hash_pk
                                ORDER BY
                                    extract_time desc
                            ) = 1
                    ) P
                WHERE
                    P.hash_pk = C.hash_pk
                    AND P.hash != C.hash
            )
        UNION
        ALL -- Query records which are deleted in new snapshot
        SELECT
            * EXCLUDE action,
            'DELETED' AS action
        FROM
            historical
        WHERE
            hash_pk NOT IN (
                SELECT
                    hash_pk
                FROM
                    staging
            ) QUALIFY RANK() OVER(
                PARTITION BY hash_pk
                ORDER BY
                    extract_time desc
            ) = 1
        UNION
        ALL -- Query records which are
        
        select
            distinct *
        from
            (
                SELECT
                    *,
                    'RECORD' AS action
                FROM
                    staging
                WHERE
                    hash_pk NOT IN (
                        SELECT
                            hash_pk
                        FROM
                            historical
                    ) QUALIFY RANK() OVER(
                        PARTITION BY hash_pk
                        ORDER BY
                            extract_time desc
                    ) = 1
            )
    ) QUALIFY RANK() OVER(
        PARTITION BY hash_pk
        ORDER BY
            extract_time desc
    ) = 1;
;

-- Kích hoạt Task
ALTER TASK IF EXISTS scd_type_6_task suspend;


SELECT * FROM latest;
SELECT * FROM historical;
select *from staging;

-- Thêm bản ghi mới
INSERT INTO staging (id, name, value, hash_pk, hash, extract_time)
VALUES
(6, 'Frank', 'Value6', 'hash6', 'hashF', '2023-10-26 09:00:00'),
(7, 'Grace', 'Value7', 'hash7', 'hashG', '2023-10-26 09:05:00');

-- Cập nhật bản ghi hiện tại
UPDATE staging
SET value = 'UpdatedValue', hash = 'hashU'
WHERE id = 3;

-- Xóa bản ghi
DELETE FROM staging
WHERE id = 2;

