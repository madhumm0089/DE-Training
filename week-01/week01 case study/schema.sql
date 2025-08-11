create database parking_toolkit;
drop database  if exists parking_toolkit;
use parking_toolkit;

drop table if exists vehicle;
drop table if exists parking_zones;
drop table if exists parking_events;

create table vehicle(
vehicle_id varchar(100) primary key ,
 plate_number varchar(100),
 vehicle_type varchar(100), 
 owner_name varchar(100)
 );
 
 insert into vehicle(vehicle_id, plate_number, vehicle_type, owner_name) values
 ('V001', 'MH12AB1234', 'sedan', 'Rahul Sharma'),
('V002', 'MH14XY9876', 'SUV', 'Neha Verma'),
('V003', 'DL01CD4567', 'hatchback', 'Aamir Sheikh'),
('V004', 'KA03ZX7788', 'SUV', 'Sneha Kulkarni'),
('V005', 'TN09QW1100', 'sedan', 'Arun Raj'),
('V006', 'MH12KL9988', 'EV', 'Manisha Pandey'),
('V007', 'GJ05HH2299', 'SUV', 'Rakesh Singh');

 
 create table parking_zones(
zone_id varchar(100) primary key,
zone_name varchar(100),
rate_per_hour int,
is_valet boolean
);

insert into parking_zones (zone_id, zone_name, rate_per_hour, is_valet) values
('Z001', 'Short Term A', 50, FALSE),
('Z002', 'Short Term B', 40, FALSE),
('Z003', 'Long Term A', 30, FALSE),
('Z004', 'Valet A', 70, TRUE),
('Z005', 'Economy Lot B', 25, FALSE);

create table parking_events(
event_id varchar(100) primary key, 
vehicle_id varchar(100)  ,
zone_id varchar(100),
entry_time datetime,
exit_time datetime, 
paid_amount float,
constraint fk_vehicle_id foreign key(vehicle_id) references vehicle(vehicle_id),
constraint fk_zone_id foreign key(zone_id) references parking_zones(zone_id)
);

insert into parking_events (event_id, vehicle_id, zone_id, entry_time, exit_time, paid_amount) values
('E001', 'V001', 'Z001', '2024-07-18 08:00:00', '2024-07-18 10:30:00', 120.0),
('E002', 'V002', 'Z002', '2024-07-18 09:00:00', '2024-07-18 11:00:00', 80.0),
('E003', 'V003', 'Z004', '2024-07-18 12:00:00', '2024-07-18 12:45:00', 70.0),
('E004', 'V001', 'Z003', '2024-07-17 15:00:00', '2024-07-18 15:00:00', 300.0),
('E005', 'V004', 'Z005', '2024-07-16 07:00:00', '2024-07-16 10:00:00', 75.0),
('E006', 'V005', 'Z003', '2024-07-15 18:00:00', '2024-07-15 18:30:00', 15.0),
('E007', 'V002', 'Z001', '2024-07-14 08:00:00', '2024-07-14 09:00:00', 50.0),
('E008', 'V006', 'Z004', '2024-07-18 10:15:00', '2024-07-18 11:15:00', 70.0),
('E009', 'V007', 'Z001', '2024-07-18 07:30:00', '2024-07-18 08:00:00', 25.0),
('E010', 'V006', 'Z005', '2024-07-17 06:00:00', '2024-07-17 08:00:00', 50.0),
('E011', 'V001', 'Z001', '2024-07-19 10:00:00', NULL, NULL),
('E012', 'V005', 'Z001', '2024-07-18 13:00:00', '2024-07-18 12:00:00', 20.0),
('E013', NULL, 'Z003', '2024-07-18 14:00:00', '2024-07-18 16:00:00', 60.0);

select * from vehicle;
select * from parking_zones;
select * from parking_events;



