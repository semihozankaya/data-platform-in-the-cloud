-- ATHENA SQL SCRIPT HERE WHICH CREATES THE `gold_allviews` TABLE
   CREATE TABLE semihozankaya_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://ozan-ceu-2020/de4/gold_allviews/'
    ) AS SELECT article, sum(views) as total_views, min(rank) as top_rank,count(rank) as ranked_days FROM semihozankaya_homework.silver_views group by article;
