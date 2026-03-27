with source as (
    select * from {{ source('tpch', 'nation') }}
),

regions as (
    select * from {{ source('tpch', 'region') }}
),

renamed as (
    select
        n.n_nationkey as nation_id,
        n.n_name as nation_name,
        n.n_regionkey as region_id,
        r.r_name as region_name,
        n.n_comment as comment
    from source n
    left join regions r on n.n_regionkey = r.r_regionkey
)

select * from renamed
