{{ config(materialized='table') }}

select
  to_date(pickup_ts)          as pickup_date,
  payment_type,
  count(*)                    as trips,
  sum(trip_distance)          as total_distance,
  sum(fare_amount)            as total_fare,
  sum(tip_amount)             as total_tip,
  sum(tolls_amount)           as total_tolls,
  round(avg(fare_amount), 2)  as avg_fare
from {{ ref('silver_taxis') }}
group by 1,2
