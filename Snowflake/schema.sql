-- Create a database (if not exists):--
CREATE WAREHOUSE IF NOT EXISTS SALES_WH

-- Use the database:--
USE WAREHOUSE SALES_WH

-- Create a database (if not exists):--
CREATE DATABASE IF NOT EXISTS HOUSE_PRICES

-- Use the database:--
USE DATABASE HOUSE_PRICES

-- Create a schema (if not exists):--
CREATE SCHEMA IF NOT EXISTS  SALES

-- Use the schema:--
USE SCHEMA SALES

-- Create the tables:--
CREATE TABLE IF NOT EXISTS CALGARY (price INT, address STRING, postal_code STRING, bed DECIMAL, full_bath DECIMAL, half_bath DECIMAL, property_area DECIMAL, property_type STRING);
CREATE TABLE IF NOT EXISTS VANCOUVER (price INT, address STRING, postal_code STRING, bed DECIMAL, full_bath DECIMAL, half_bath DECIMAL, property_area DECIMAL, property_type STRING);

-- Insert data into the tables:--
