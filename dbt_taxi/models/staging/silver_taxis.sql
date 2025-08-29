{{ config(materialized='view') }}

select * from hive_metastore.default.silver_taxis
