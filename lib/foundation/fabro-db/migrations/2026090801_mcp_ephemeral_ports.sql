-- Port zero requests an OS-assigned, per-agent sandbox listener.
ALTER TABLE mcp_servers RENAME TO mcp_servers_fixed_port;

CREATE TABLE mcp_servers (
    id TEXT PRIMARY KEY NOT NULL,
    revision TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    transport_type TEXT NOT NULL,
    protocol TEXT,
    command_json TEXT,
    url TEXT,
    port INTEGER,
    env_json TEXT,
    headers_json TEXT,
    startup_timeout_secs INTEGER NOT NULL,
    tool_timeout_secs INTEGER NOT NULL,
    CHECK (length(id) BETWEEN 1 AND 63),
    CHECK (substr(id, 1, 1) GLOB '[a-z0-9]'),
    CHECK (id NOT GLOB '*[^a-z0-9-]*'),
    CHECK (length(revision) = 64),
    CHECK (revision NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(trim(display_name)) > 0),
    CHECK (transport_type IN ('stdio', 'http', 'sandbox')),
    CHECK (protocol IS NULL OR protocol IN ('streamable_http', 'sse')),
    CHECK (startup_timeout_secs >= 0),
    CHECK (tool_timeout_secs >= 0),
    CHECK (
        (
            transport_type = 'stdio'
            AND protocol IS NULL
            AND command_json IS NOT NULL
            AND json_valid(command_json)
            AND json_type(command_json) = 'array'
            AND json_array_length(command_json) > 0
            AND json_type(command_json, '$[0]') = 'text'
            AND length(trim(json_extract(command_json, '$[0]'))) > 0
            AND url IS NULL
            AND port IS NULL
            AND env_json IS NOT NULL
            AND json_valid(env_json)
            AND json_type(env_json) = 'object'
            AND headers_json IS NULL
        )
        OR
        (
            transport_type = 'http'
            AND protocol IS NOT NULL
            AND command_json IS NULL
            AND url IS NOT NULL
            AND length(trim(url)) > 0
            AND port IS NULL
            AND env_json IS NULL
            AND headers_json IS NOT NULL
            AND json_valid(headers_json)
            AND json_type(headers_json) = 'object'
        )
        OR
        (
            transport_type = 'sandbox'
            AND protocol IS NOT NULL
            AND command_json IS NOT NULL
            AND json_valid(command_json)
            AND json_type(command_json) = 'array'
            AND json_array_length(command_json) > 0
            AND json_type(command_json, '$[0]') = 'text'
            AND length(trim(json_extract(command_json, '$[0]'))) > 0
            AND url IS NULL
            AND port BETWEEN 0 AND 65535
            AND env_json IS NOT NULL
            AND json_valid(env_json)
            AND json_type(env_json) = 'object'
            AND headers_json IS NULL
        )
    )
);

INSERT INTO mcp_servers SELECT * FROM mcp_servers_fixed_port;
DROP TABLE mcp_servers_fixed_port;
