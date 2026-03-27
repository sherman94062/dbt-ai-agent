with source as (
    select * from {{ source('tpch', 'part') }}
),

renamed as (
    select
        p_partkey as part_id,
        p_name as part_name,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_type as part_type,
        p_size as size,
        p_container as container,
        p_retailprice::numeric as retail_price,
        p_comment as comment
    from source
)

select * from renamed
