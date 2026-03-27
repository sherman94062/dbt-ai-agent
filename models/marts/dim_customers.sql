{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

nations as (
    select * from {{ ref('stg_nations') }}
),

final as (
    select
        c.customer_id,
        c.customer_name,
        c.address,
        c.phone,
        c.account_balance,
        c.market_segment,
        n.nation_id,
        n.nation_name,
        n.region_id,
        n.region_name
    from customers c
    inner join nations n on c.nation_id = n.nation_id
)

select * from final
