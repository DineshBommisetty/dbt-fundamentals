with stg_payments as (
    select
    ID, ORDERID, PAYMENTMETHOD, STATUS, AMOUNT/100 as amount, CREATED, _BATCHED_AT

from {{source('stripe','payment')}}
)
select * from stg_payments