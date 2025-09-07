{{ config(materialized='table') }}

select
  pickup_date,
  payment_type,
  count(*)                    as trips,
  sum(trip_distance)          as total_distance,
  sum(fare_amount)            as total_fare,
  sum(tip_amount)             as total_tip,
  sum(tolls_amount)           as total_tolls,
  sum(total_amount)           as total_amount,
  round(avg(fare_amount), 2)  as avg_fare,
  round(avg(trip_distance), 2) as avg_distance,
  round(avg(total_amount), 2) as avg_total
from {{ ref('silver_taxis') }}
group by 1,2
