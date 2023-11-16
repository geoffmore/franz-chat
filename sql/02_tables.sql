-- Use franz DB

-- CREATE TABLE IF NOT EXISTS users (
CREATE TABLE IF NOT EXISTS users (
    -- Primary key is a combo of unique and not null
    uuid uuid PRIMARY KEY,
    name varchar(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS channels (
    uuid uuid PRIMARY KEY,
    name varchar(10) NOT NULL UNIQUE
    -- TODO add owner field from users for channel ownership
);

CREATE ROLE franz_api WITH LOGIN PASSWORD 'franz_api';

ALTER TABLE users OWNER TO franz_api;
ALTER TABLE channels OWNER to franz_api;
