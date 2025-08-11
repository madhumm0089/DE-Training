use parking_toolkit;

--- top perfoming zones by revenue
select z.zone_id, z.zone_name, sum(p.paid_amount) as total_revenue
from parking_events p 
join parking_zones z
on p.zone_id = z.zone_id
group by z.zone_id, z.zone_name
order by  total_revenue desc;

--- list of frequent visit vehicles
select v.vehicle_id,  v.owner_name, count(pe.vehicle_id) as total_visits
from parking_events pe
join vehicle v
on pe.vehicle_id = v.vehicle_id
group by v.vehicle_id,  v.owner_name
order by total_visits desc;

--- list of vehicle with longest duration of parking

select v.vehicle_id,  v.owner_name,
round(sum(timestampdiff(minute,pe.entry_time, pe.exit_time)) / 60, 2) as total_hours
from parking_events pe
join vehicle v
on pe.vehicle_id = v.vehicle_id
where pe.entry_time is not null and pe.exit_time is not null
group by v.vehicle_id,  v.owner_name
order by total_hours desc ;

--- to detect where vehicle id is null
select * from parking_events where vehicle_id is null; 

--- Invalid timestamp
select * from parking_events
where entry_time > exit_time;

--- compare zones by revenue
select z.is_valet, sum(pe.paid_amount) as total_revenue
from parking_events pe
join parking_zones z
on z.zone_id = pe.zone_id
group by z.is_valet;



