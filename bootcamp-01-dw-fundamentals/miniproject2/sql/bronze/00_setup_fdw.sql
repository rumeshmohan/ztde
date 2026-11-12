-- ============================================================
-- BRONZE LAYER - CROSS-DATABASE ACCESS SETUP
--
-- Both abc_hub_operational (source) and abc_hub_analytical (target)
-- are PostgreSQL, so extraction does not need a second JDBC hop the
-- way Mini Project 01's NiFi flow did. postgres_fdw exposes the
-- operational tables as foreign tables inside abc_hub_analytical, so
-- every Bronze extraction task in the Airflow DAG is a single plain
-- SQL statement against ONE connection (postgres_analytical) - no
-- custom Python needed to move rows between two connections.
--
-- Run this in: abc_hub_analytical (as a superuser, once)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER IF NOT EXISTS abc_hub_operational_srv
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'localhost', port '5432', dbname 'abc_hub_operational');

-- Replace the user and CHANGE_ME with the role (and its password) that
-- Airflow's postgres_analytical connection actually authenticates as in
-- your environment. Do not commit a real password.
CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
    SERVER abc_hub_operational_srv
    OPTIONS (user 'postgres', password 'CHANGE_ME');

CREATE SCHEMA IF NOT EXISTS source;

-- One foreign table per operational table the Bronze layer extracts
-- from (the 13 sources Mini Project 01 already staged, plus the 7
-- small lookup/join tables NiFi previously joined against live at
-- extraction time - country, city, content_type, genre,
-- content_genre, customer_address, customer_subscription).
IMPORT FOREIGN SCHEMA public
    LIMIT TO (
        country, city, content_type, genre, device, subscription_plan,
        support_category, warehouse, customer, customer_address,
        customer_subscription, content, content_genre, inventory_item,
        streaming_session, rental, payment, support_ticket, review, wishlist
    )
    FROM SERVER abc_hub_operational_srv
    INTO source;
