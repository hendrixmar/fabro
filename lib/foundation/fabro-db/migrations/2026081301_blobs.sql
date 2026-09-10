CREATE TABLE blobs (
    hash TEXT PRIMARY KEY NOT NULL,
    data BLOB NOT NULL,
    CHECK (length(hash) = 64),
    CHECK (hash = lower(hash)),
    CHECK (hash NOT GLOB '*[^0-9a-f]*')
);
