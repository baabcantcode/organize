-- Add migration script here
CREATE TABLE IF NOT EXISTS todos
(
    id          INTEGER PRIMARY KEY NOT NULL,
    description TEXT                NOT NULL,
    done        TEXT CHECK( done IN ('U','P','D') )   NOT NULL DEFAULT 'U'
);
