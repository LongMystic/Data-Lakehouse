-- Create Ranger database
CREATE DATABASE IF NOT EXISTS ranger;

-- Create Ranger user
CREATE USER IF NOT EXISTS 'ranger'@'%' IDENTIFIED BY 'ranger';

-- Grant privileges to Ranger user on Ranger database
GRANT ALL PRIVILEGES ON ranger.* TO 'ranger'@'%';

-- Flush privileges to apply changes
FLUSH PRIVILEGES; 