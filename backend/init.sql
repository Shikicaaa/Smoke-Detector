create table sensor_data (
    time timestamptz not null,
    

    temperature_c double precision,
    humidity_percent double precision,
    tvoc_ppb integer,
    eco2_ppm integer,
    raw_h2 integer,
    raw_ethanol integer,
    pressure_hpa double precision,
    pm10 double precision,
    pm25 double precision,
    nc05 double precision,
    nc10 double precision,
    nc25 double precision,
    fire_alarm boolean,
    PRIMARY KEY (time)
);

select create_hypertable('sensor_data', 'time');

create index if not exists ix_temperature_c on sensor_data (temperature_c);
create index if not exists ix_humidity_percent on sensor_data (humidity_percent);
create index if not exists ix_pressure_hpa on sensor_data (pressure_hpa);
create index if not exists ix_fire_alarm on sensor_data (fire_alarm) where fire_alarm = true;