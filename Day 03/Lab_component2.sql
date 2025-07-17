use ecommerce;

--- task1
select c.id,(select o.amount from orders o where c.id = o.customer_id order by o.order_date desc limit 1) as recent_order_amount from customers c ;
--- task2
select count(*) as total_orders from orders;
select count(*) as total_orders_above_5000 from orders where amount > 5000;
select count(*) as total_orders_below_1000 from orders where amount < 1000;

--- task3


select o.customer_id,count(*) as highest_num_ord
from orders o 
group by o.customer_id
order by highest_num_ord desc limit 2;

select * from orders;