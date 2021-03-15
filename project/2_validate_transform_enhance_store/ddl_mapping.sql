


drop table if exists BreadCrumb;
drop table if exists Trip;
drop type if exists service_type;
drop type if exists tripdir_type;

create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
create type tripdir_type as enum ('Out', 'Back');

create table Trip (
        trip_id integer,
        route_id integer,
        vehicle_id integer,
        service_key service_type,
        direction tripdir_type,
        PRIMARY KEY (trip_id)
);

create table BreadCrumb (
        tstamp timestamp,
        latitude float,
        longitude float,
        direction integer,
        speed float,
        trip_id integer,
        FOREIGN KEY (trip_id) REFERENCES Trip
);



ctran=> \d breadcrumb
                        Table "public.breadcrumb"
  Column   |            Type             | Collation | Nullable | Default 
-----------+-----------------------------+-----------+----------+---------
 tstamp    | timestamp without time zone |           |          | 
 latitude  | double precision            |           |          | 
 longitude | double precision            |           |          | 
 direction | integer                     |           |          | 
 speed     | double precision            |           |          | 
 trip_id   | integer                     |           |          | 
Foreign-key constraints:
    "breadcrumb_trip_id_fkey" FOREIGN KEY (trip_id) REFERENCES trip(trip_id)


ctran=> \d trip;
                     Table "public.trip"
   Column    |     Type     | Collation | Nullable | Default 
-------------+--------------+-----------+----------+---------
 trip_id     | integer      |           | not null | 
 route_id    | integer      |           |          | 
 vehicle_id  | integer      |           |          | 
 service_key | service_type |           |          | 
 direction   | tripdir_type |           |          | 
Indexes:
    "trip_pkey" PRIMARY KEY, btree (trip_id)
Referenced by:
    TABLE "breadcrumb" CONSTRAINT "breadcrumb_trip_id_fkey" FOREIGN KEY (trip_id) REFERENCES trip(trip_id)   



/*
* Example BreadCrumb:
*     "EVENT_NO_TRIP": "170704359",
*     "EVENT_NO_STOP": "170704381",
*     "OPD_DATE": "19-OCT-20",
*     "VEHICLE_ID": "1298380",
*     "METERS": "210068",
*     "ACT_TIME": "82316",
*     "VELOCITY": "8",
*     "DIRECTION": "18",
*     "RADIO_QUALITY": "",
*     "GPS_LONGITUDE": "-122.657817",
*     "GPS_LATITUDE": "45.690745",
*     "GPS_SATELLITES": "12",
*     "GPS_HDOP": "0.7",
*     "SCHEDULE_DEVIATION": ""
*/

/*
* BreadCrumb to table values mappings
* 
* Table Trip
* trip_id = int(EVENT_NO_TRIP) # cross populate to breadcrumb
* route_id = 0 # for now cannot populate this yet. in pp2
* vehicle_id = int(VEHICLE_ID)
* service_key = service_type('Weekday', 'Saturday', 'Sunday') # determine as dow(timestamp), not enough info to determine if holiday in pp2
* direction = 'Out' # tripdir_type('Out', 'Back') # not direction from breadcrumb, not enough info to populate yet.
* 
* Table BreadCrumb
* timestamp = timestamp(OPD_DATE) + unix_timestamp(ACT_TIME)
* latitude = float(GPS_LATITUDE)
* longitude = float(GPS_LONGITUDE)
* direction = int(DIRECTION)
* speed = float(VELOCITY)
* trip_id = int(EVENT_NO_TRIP) #reference trip 
*/


/* StopEvent to Trip column
* 'trip_id' = trip_id
* 'route_number' = route_id
* 'vehicle_number' = vehicle_id
* 'service_key' = service_key
* 'direction' = direction
*/
