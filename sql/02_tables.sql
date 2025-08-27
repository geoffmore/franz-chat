-- Use schema franz_chat.public

CREATE TABLE IF NOT EXISTS users (
    -- Primary key is a combo of unique and not null
    uuid uuid PRIMARY KEY,
    name varchar(20) NOT NULL UNIQUE -- TODO - return error on name collision
);

CREATE TABLE IF NOT EXISTS channels (
    uuid uuid PRIMARY KEY,
    name varchar(20) NOT NULL UNIQUE
    -- TODO add owner field from users for channel ownership
);

CREATE TABLE IF NOT EXISTS messages (
    uuid uuid PRIMARY KEY,
    timestamp timestamp with time zone NOT NULL, -- implicit UTC with automatic conversion
    message varchar(140) NOT NULL -- based on Twitter original char length
);

CREATE TABLE IF NOT EXISTS metrics (
    foo serial PRIMARY KEY
);

-- I'm too lazy to alter the users table, so I'm going to use a foreign key instead
CREATE TABLE IF NOT EXISTS auth (
    uuid uuid references users(uuid),
    password VARCHAR(20) NOT NULL -- TODO add character class constraints
);

CREATE EXTENSION pgcrypto;
