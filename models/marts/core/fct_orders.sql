with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

final as (

    select
        payments.ORDERID,
        coalesce(orders.customer_id,-999) as customer_id,
        coalesce(case when payments.status = 'success' then payments.AMOUNT end , 0) as AMOUNT

    from payments

    left join orders 
        on payments.ORDERID = orders.order_id

)

select * from final