-- Initial database setup
-- This file will be executed when the PostgreSQL container starts

-- Create database if it doesn't exist (though docker-compose already creates it)
-- CREATE DATABASE IF NOT EXISTS piscineds;

-- Connect to the database
\c piscineds;

-- Create a test table to verify connection
CREATE TABLE IF NOT EXISTS connection_test (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    message VARCHAR(100) DEFAULT 'Database connection successful'
);

INSERT INTO connection_test (message) VALUES ('PostgreSQL is ready!');