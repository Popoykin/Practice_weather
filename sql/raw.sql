create schema if not exists raw;

create table if not exists api_dummyjson
(
  id integer,
  firstname text,
  lastname text,
  age integer,
  gender text,
  country text,
  companyname text,
  update_at timestamp,
  load_at timestamp
);