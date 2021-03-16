select longitude, latitude, speed 
from breadcrumb b left join 
trip t on b.trip_id = t.trip_id
where route_id = 65 
and t.direction = 'Out'
and tstamp between '2020-10-18 16:00:00' and '2020-10-18 18:00:00'
order by tstamp;