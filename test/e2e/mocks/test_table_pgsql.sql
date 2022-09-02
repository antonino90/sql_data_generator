CREATE TYPE enum_type AS ENUM ('value1', 'value2', 'value3');

CREATE TABLE test_types (
        uuid uuid,
        smallint smallint,
        int int,
        integer integer,
        bigint bigint,
        bigserial bigserial,
        real real,
        double double precision,
        float float,
        float8 float8,
        decimal decimal,
        numeric numeric,
        char char,
        varchar varchar(255),
        date date,
        time time,
        timestamp timestamp,
        timestamp_with_time_zone timestamp with time zone,
        timestamp_without_time_zone timestamp without time zone,
        character character,
        character_varying character varying,
        text text,
        enum enum_type,
        array_column text[]
);




# not implemented colum type
# bit bit