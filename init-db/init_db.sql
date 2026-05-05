create database postgres_practice_gpt;

create schema if not exists raw;

create table if not exists raw.cities (
	city text NULL,
	country_code text NULL
);

CREATE TABLE if not exists raw.city_coords (
	"name" varchar(100) NOT NULL,
	lat float8 NULL,
	lon float8 NULL,
	country_code varchar(10) NOT NULL,
	update_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT city_coords_pkey PRIMARY KEY (name, country_code)
);

CREATE TABLE if not exists raw.actual_weather (
	city varchar(100) NULL,
	"date" date NULL,
	max_temp float8 NULL,
	min_temp float8 NULL,
	precipitation_forecast float8 NULL,
	loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT actual_weather_city_date_key UNIQUE (city, date)
);

CREATE TABLE if not exists raw.forecasted_weather (
	city varchar(100) NOT NULL,
	"date" date NOT NULL,
	max_temp float8 NULL,
	min_temp float8 NULL,
	precipitation_forecast float8 NULL,
	date_receipt_of_forecast date DEFAULT CURRENT_DATE NOT NULL,
	loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT forecasted_weather_pkey PRIMARY KEY (city, date, date_receipt_of_forecast)
);