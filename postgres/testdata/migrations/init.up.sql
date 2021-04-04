begin;

create table names
(
    name      varchar(36)           not null,
    processed boolean default false not null,
    id        serial                not null
);

create table cities
(
    country    text,
    value      numeric     not null,
    start_date timestamp   not null,
    end_date   timestamp
);

commit;