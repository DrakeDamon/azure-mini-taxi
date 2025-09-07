{{ config(materialized='view') }}

with source_data as (
    select 
        pickup_ts,
        dropoff_ts,
        trip_distance,
        fare_amount,
        tip_amount,
        tolls_amount,
        payment_type
    from {{ source('default', 'silver_taxis') }}
),

staged as (
    select
        -- Create unique row-based identifier
        row_number() over (order by pickup_ts, trip_distance, fare_amount) as trip_id,
        
        -- Convert timestamps
        cast(pickup_ts as timestamp) as pickup_ts,
        cast(dropoff_ts as timestamp) as dropoff_ts,
        
        -- Trip metrics
        trip_distance,
        fare_amount,
        coalesce(tip_amount, 0) as tip_amount,
        coalesce(tolls_amount, 0) as tolls_amount,
        
        -- Standardize payment type
        case 
            when lower(payment_type) in ('cash', 'cas') then 'Cash'
            when lower(payment_type) in ('credit', 'crd', 'credit card') then 'Credit'
            else 'Other'
        end as payment_type,
        
        -- Calculate total amount
        fare_amount + coalesce(tip_amount, 0) + coalesce(tolls_amount, 0) as total_amount,
        
        -- Extract date for partitioning
        date(cast(pickup_ts as timestamp)) as pickup_date
        
    from source_data
    where pickup_ts is not null
      and fare_amount >= 0
      and trip_distance > 0
)

select * from staged
