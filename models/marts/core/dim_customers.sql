with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (

    select * from {{ ref('stg_orders') }}

),
payments as (

    select * from {{ ref('stg_payments') }}

),
customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),
value1 as (
    select
        orders.customer_id as customer_id,
        sum(coalesce(payments.AMOUNT, 0)) as lifetime_value

    from orders
    left join payments 
        on payments.ORDERID = orders.order_id
    group by orders.customer_id
),
final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        value1.lifetime_value
    from customers

    left join customer_orders using (customer_id)
    left join value1 using (customer_id)
)

select * from final