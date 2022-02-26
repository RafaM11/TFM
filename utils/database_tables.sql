CREATE TYPE coordinates AS (
    latitude FLOAT, 
    longitude FLOAT
);

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS tb_links (
    link_id INT PRIMARY KEY,
    borough VARCHAR(13) NOT NULL,
    points coordinates[] NOT NULL,
    distance FLOAT NOT NULL,
    encoded_poly_line VARCHAR (255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS tb_historical_traffic (
    id_measurement BIGSERIAL PRIMARY KEY,
    link_id INT NOT NULL REFERENCES tb_links(link_id),
    speed FLOAT NOT NULL, 
    travel_time INT NOT NULL, 
    measurement_date TIMESTAMP NOT NULL 
);

CREATE TABLE IF NOT EXISTS tb_real_time_traffic (
    id_measurement SERIAL PRIMARY KEY,
    link_id INT NOT NULL REFERENCES tb_links(link_id),
    speed FLOAT NOT NULL, 
    travel_time INT NOT NULL,
    measurement_date TIMESTAMP NOT NULL
);