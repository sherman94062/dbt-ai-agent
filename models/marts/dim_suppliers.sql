{{
    config(
        materialized='table'
    )
}}

with suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

nations as (
    select * from {{ ref('stg_nations') }}
),

final as (
    select
        s.supplier_id,
        s.supplier_name,
        s.address,
        s.phone,
        s.account_balance,
        n.nation_id,
        n.nation_name,
        n.region_id,
        n.region_name
    from suppliers s
    inner join nations n on s.nation_id = n.nation_id
)

select * from final
