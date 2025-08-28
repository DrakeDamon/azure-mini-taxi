select
  date_trunc('day', pickup_ts) as pickup_date,
  count(*)         as trips,
  sum(fare_amount) as total_fare,
  sum(tip_amount)  as total_tip,
  avg(fare_amount) as avg_fare
from {{ ref('silver_taxis') }}
group by 1