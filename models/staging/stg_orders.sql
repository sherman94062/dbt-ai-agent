with source as (
    select * from {{ source('tpch', 'orders') }}
),

renamed as (
    select
        o_orderkey as order_id,
        o_custkey as customer_id,
        o_orderstatus as order_status,
        o_totalprice::numeric as total_price,
        o_orderdate::date as order_date,
        o_orderpriority as order_priority,
        o_clerk as clerk,
        o_shippriority as ship_priority,
        o_comment as comment
    from source
)

select * from renamed
