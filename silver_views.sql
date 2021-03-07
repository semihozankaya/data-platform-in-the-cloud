-- ATHENA SQL SCRIPT HERE WHICH CREATES THE `silver_views` TABLE
   CREATE TABLE semihozankaya_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://ozan-ceu-2020/de4/silver_views/'
    ) AS SELECT article, views, rank, date FROM semihozankaya_homework.bronze_views;
