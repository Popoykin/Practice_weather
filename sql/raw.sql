create schema if not exists raw;

create table if not exists raw.api_dummyjson
(
  id integer,
  user_json JSONB,
  loaded_at timestamp
);

create table if not exists raw.etl_state
(
  id integer,
  last_upadate timestamp,
  last_load_at timestamp,

)