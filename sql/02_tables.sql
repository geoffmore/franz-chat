-- Use schema franz_chat.public

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

CREATE TABLE IF NOT EXISTS messages (
    uuid uuid PRIMARY KEY,
    message varchar(140) -- based on Twitter original char length
);

