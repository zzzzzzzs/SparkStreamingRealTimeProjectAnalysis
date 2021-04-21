
from (
select *
from (
    select  order_id
    from order_detail
) t1
join order_info t2
on t1.order_id = t2.id
) t3

