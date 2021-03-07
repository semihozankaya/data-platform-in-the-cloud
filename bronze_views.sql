-- ATHENA SQL SCRIPT HERE WHICH CREATES THE `bronze_views` TABLE
 CREATE EXTERNAL TABLE
semihozankaya_homework.bronze_views (
    article STRING,
    views INT,
    rank INT, 
    date DATE,
    retrieved_at TIMESTAMP) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://ozan-ceu-2020/de4/views/';
