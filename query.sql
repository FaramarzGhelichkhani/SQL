with log as (select network,network as net,number,organization,null as id from asn_data
union 
select ip,null,null,null,id from ip_data
order by 1),

log2 as (select *,
case when network <<= first_value(net) over (partition by value_partition order by network) then 
first_value(number) over (partition by value_partition order by network)
else number 
end as asn_num,

case when network <<= first_value(net) over (partition by value_partition order by network) then 
first_value(organization) over (partition by value_partition order by network)
else number 
end as organization_name
		 
from (select *,
sum(case when net is null then 0 else 1 end) over (order by network) as value_partition 
from log
order by network) as q)

select id,network, asn_num, organization_name from log2
where number is null 
order by id
--------------------------------------------------------------------------------------
select actor.first_name || ' ' || actor.last_name as full_name
from city
join address
on city.city_id = address.city_id and  city='Esfahan'
join customer
on address.address_id = customer.address_id
join rental
on customer.customer_id = rental.customer_id
join inventory
on inventory.inventory_id = rental.inventory_id
join film_actor
on film_actor.film_id = inventory.film_id
join actor
on film_actor.actor_id = actor.actor_id 
group by actor.actor_id
order by count(actor.actor_id) desc
limit 1
----------------------------------------------------------------------------------------
