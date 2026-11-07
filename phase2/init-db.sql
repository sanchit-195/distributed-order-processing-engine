-- Creates microservice databases if they do not already exist
SELECT 'CREATE DATABASE inventorydb'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'inventorydb')\gexec

SELECT 'CREATE DATABASE paymentdb'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'paymentdb')\gexec

