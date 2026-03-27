{{
    config(
        materialized='table'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

lineitems as (
    select * from {{ ref('stg_lineitems') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

nations as (
    select * from {{ ref('stg_nations') }}
),

order_items as (
    select
        -- identifiers
        li.order_id,
        li.line_number,
        li.part_id,
        li.supplier_id,
        o.customer_id,

        -- customer dimensions
        c.customer_name,
        c.market_segment,
        n.nation_id as customer_nation_id,
        n.nation_name as customer_nation,
        n.region_name as customer_region,

        -- order dimensions
        o.order_date,
        o.order_status,
        o.order_priority,

        -- lineitem dimensions
        li.ship_date,
        li.commit_date,
        li.receipt_date,
        li.return_flag,
        li.line_status,
        li.ship_mode,

        -- measures
        li.quantity,
        li.extended_price,
        li.discount,
        li.tax,
        li.line_revenue,
        li.line_revenue_with_tax,
        o.total_price as order_total_price,

        -- derived measures
        li.ship_date - o.order_date as fulfillment_days

    from lineitems li
    inner join orders o on li.order_id = o.order_id
    inner join customers c on o.customer_id = c.customer_id
    inner join nations n on c.nation_id = n.nation_id
)

select * from order_items
