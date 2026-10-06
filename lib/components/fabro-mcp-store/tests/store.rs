#![expect(
    clippy::unwrap_used,
    reason = "SQLite MCP store integration tests use panic-on-failure fixture setup"
)]

use std::collections::HashMap;
use std::error::Error as _;

use fabro_db::Database;
use fabro_mcp_store::{McpServerStore, McpServerStoreError, import_legacy_directory_once};
use fabro_types::settings::run::{McpHttpProtocol, McpTransport};
use fabro_types::{McpServerDraft, McpServerId, McpServerReplace, McpServerRevision};
use tokio::fs;

async fn test_database() -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::connect(dir.path().join("fabro.sqlite3"))
        .await
        .unwrap();
    database.migrate().await.unwrap();
    (dir, database)
}

fn http_transport(url: &str) -> McpTransport {
    McpTransport::Http {
        protocol: McpHttpProtocol::default(),
        url:      url.to_string(),
        headers:  HashMap::new(),
    }
}

fn draft(id: &str, display_name: &str) -> McpServerDraft {
    McpServerDraft {
        id:                   McpServerId::new(id).unwrap(),
        display_name:         display_name.to_string(),
        description:          None,
        transport:            http_transport("https://example.com/mcp"),
        startup_timeout_secs: 10,
        tool_timeout_secs:    60,
    }
}

fn replacement(display_name: &str) -> McpServerReplace {
    McpServerReplace {
        display_name:         display_name.to_string(),
        description:          Some("updated".to_string()),
        transport:            http_transport("https://example.com/mcp/v2"),
        startup_timeout_secs: 15,
        tool_timeout_secs:    90,
    }
}

fn legacy_toml(display_name: &str, url: &str) -> String {
    format!(
        r#"display_name = "{display_name}"
startup_timeout_secs = 10
tool_timeout_secs = 60

[transport]
type = "http"
url = "{url}"

[transport.headers]
"#
    )
}

#[tokio::test]
async fn empty_database_loads_empty_store() {
    let (_dir, database) = test_database().await;
    let store = McpServerStore::load(database.clone_pool()).await.unwrap();

    assert!(store.list().is_empty());
    assert!(store.ids().is_empty());
}

#[tokio::test]
async fn create_get_list_replace_delete_and_reload_round_trip() {
    let (_dir, database) = test_database().await;
    let store = McpServerStore::load(database.clone_pool()).await.unwrap();

    let created = store.create(draft("sentry", "Sentry")).await.unwrap();
    assert_eq!(store.get(&created.id).unwrap(), created);
    assert_eq!(store.list(), vec![created.clone()]);
    assert_eq!(store.ids(), vec![created.id.clone()]);

    let reloaded = McpServerStore::load(database.clone_pool()).await.unwrap();
    assert_eq!(reloaded.get(&created.id).unwrap(), created);

    let replaced = store
        .replace(&created.id, &created.revision, replacement("Sentry v2"))
        .await
        .unwrap();
    assert_ne!(replaced.revision, created.revision);
    assert_eq!(replaced.display_name, "Sentry v2");

    store
        .delete(&replaced.id, &replaced.revision)
        .await
        .unwrap();
    assert!(store.get(&replaced.id).is_none());
    assert!(
        McpServerStore::load(database.clone_pool())
            .await
            .unwrap()
            .list()
            .is_empty()
    );
}

#[tokio::test]
async fn sandbox_ephemeral_port_survives_catalog_reload() {
    let (_dir, database) = test_database().await;
    let store = McpServerStore::load(database.clone_pool()).await.unwrap();
    let mut definition = draft("debugger", "Debugger");
    definition.transport = McpTransport::Sandbox {
        protocol: McpHttpProtocol::StreamableHttp,
        command:  vec!["debugger".to_string()],
        port:     0,
        env:      HashMap::new(),
    };
    let created = store.create(definition).await.unwrap();
    let reloaded = McpServerStore::load(database.clone_pool()).await.unwrap();
    assert!(matches!(
        reloaded.get(&created.id).unwrap().transport,
        McpTransport::Sandbox { port: 0, .. }
    ));
}

#[tokio::test]
async fn all_transport_variants_round_trip_with_sorted_map_json() {
    let (_dir, database) = test_database().await;
    let store = McpServerStore::load(database.clone_pool()).await.unwrap();
    let definitions = [
        McpServerDraft {
            id:                   McpServerId::new("stdio").unwrap(),
            display_name:         "Stdio".to_string(),
            description:          None,
            transport:            McpTransport::Stdio {
                command: vec!["server".to_string(), "--flag".to_string()],
                env:     HashMap::from([
                    ("Z_KEY".to_string(), "z".to_string()),
                    ("A_KEY".to_string(), "a".to_string()),
                ]),
            },
            startup_timeout_secs: 1,
            tool_timeout_secs:    2,
        },
        McpServerDraft {
            id:                   McpServerId::new("http").unwrap(),
            display_name:         "HTTP".to_string(),
            description:          None,
            transport:            McpTransport::Http {
                protocol: McpHttpProtocol::Sse,
                url:      "https://example.com/mcp".to_string(),
                headers:  HashMap::from([("Authorization".to_string(), "secret".to_string())]),
            },
            startup_timeout_secs: 3,
            tool_timeout_secs:    4,
        },
        McpServerDraft {
            id:                   McpServerId::new("sandbox").unwrap(),
            display_name:         "Sandbox".to_string(),
            description:          None,
            transport:            McpTransport::Sandbox {
                protocol: McpHttpProtocol::StreamableHttp,
                command:  vec!["server".to_string()],
                port:     3000,
                env:      HashMap::from([("TOKEN".to_string(), "secret".to_string())]),
            },
            startup_timeout_secs: 5,
            tool_timeout_secs:    6,
        },
    ];

    for definition in definitions.clone() {
        store.create(definition).await.unwrap();
    }
    let reloaded = McpServerStore::load(database.clone_pool()).await.unwrap();
    for expected in definitions {
        let actual = reloaded.get(&expected.id).unwrap();
        assert_eq!(actual.display_name, expected.display_name);
        assert_eq!(actual.transport, expected.transport);
    }

    let env_json: String =
        sqlx::query_scalar("SELECT env_json FROM mcp_servers WHERE id = 'stdio'")
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(env_json, r#"{"A_KEY":"a","Z_KEY":"z"}"#);
}

#[tokio::test]
async fn duplicate_create_is_rejected() {
    let (_dir, database) = test_database().await;
    let first = McpServerStore::load(database.clone_pool()).await.unwrap();
    let second = McpServerStore::load(database.clone_pool()).await.unwrap();
    first.create(draft("sentry", "Sentry")).await.unwrap();

    let err = second
        .create(draft("sentry", "Duplicate"))
        .await
        .unwrap_err();
    assert!(matches!(err, McpServerStoreError::AlreadyExists { .. }));
}

#[tokio::test]
async fn revision_guard_rejects_stale_independent_store() {
    let (_dir, database) = test_database().await;
    let creator = McpServerStore::load(database.clone_pool()).await.unwrap();
    let created = creator.create(draft("sentry", "Sentry")).await.unwrap();
    let first = McpServerStore::load(database.clone_pool()).await.unwrap();
    let second = McpServerStore::load(database.clone_pool()).await.unwrap();

    first
        .replace(&created.id, &created.revision, replacement("Winner"))
        .await
        .unwrap();
    let err = second
        .replace(&created.id, &created.revision, replacement("Loser"))
        .await
        .unwrap_err();

    assert!(matches!(err, McpServerStoreError::StaleRevision { .. }));
    assert_eq!(second.get(&created.id).unwrap(), created);
}

#[tokio::test]
async fn imports_legacy_directory_once_without_overwriting_sql() {
    let (dir, database) = test_database().await;
    let store = McpServerStore::load(database.clone_pool()).await.unwrap();
    store.create(draft("existing", "SQLite")).await.unwrap();
    let source = dir.path().join("mcps");
    fs::create_dir_all(&source).await.unwrap();
    let existing_bytes = legacy_toml("Legacy", "https://legacy.example.com/mcp");
    let imported_bytes = legacy_toml("Imported", "https://new.example.com/mcp");
    fs::write(source.join("existing.toml"), &existing_bytes)
        .await
        .unwrap();
    fs::write(source.join("new.toml"), &imported_bytes)
        .await
        .unwrap();
    fs::write(source.join("notes.txt"), "preserve in backup")
        .await
        .unwrap();

    let report = import_legacy_directory_once(database.pool(), &source)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(report.imported_rows, 1);
    assert_eq!(report.skipped_rows, 1);
    assert_eq!(report.names, vec!["new"]);
    assert!(!source.exists());
    assert!(report.backup_path.join("notes.txt").exists());
    let reloaded = McpServerStore::load(database.clone_pool()).await.unwrap();
    assert_eq!(
        reloaded
            .get(&McpServerId::new("existing").unwrap())
            .unwrap()
            .display_name,
        "SQLite"
    );
    let imported = reloaded.get(&McpServerId::new("new").unwrap()).unwrap();
    assert_eq!(imported.display_name, "Imported");
    assert_eq!(
        imported.revision,
        McpServerRevision::from_bytes(imported_bytes.as_bytes())
    );
    assert!(
        import_legacy_directory_once(database.pool(), &source)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn malformed_legacy_toml_does_not_import_or_rename() {
    let (dir, database) = test_database().await;
    let source = dir.path().join("mcps");
    fs::create_dir_all(&source).await.unwrap();
    fs::write(
        source.join("valid.toml"),
        legacy_toml("Valid", "https://example.com/mcp"),
    )
    .await
    .unwrap();
    fs::write(source.join("broken.toml"), "not valid toml =")
        .await
        .unwrap();

    let err = import_legacy_directory_once(database.pool(), &source)
        .await
        .unwrap_err();

    assert!(matches!(err, McpServerStoreError::Parse { .. }));
    assert!(source.exists());
    assert!(
        McpServerStore::load(database.clone_pool())
            .await
            .unwrap()
            .list()
            .is_empty()
    );
}

#[tokio::test]
async fn legacy_parse_error_chain_does_not_expose_transport_values() {
    let (dir, database) = test_database().await;
    let source = dir.path().join("mcps");
    fs::create_dir_all(&source).await.unwrap();
    fs::write(
        source.join("broken.toml"),
        r#"
display_name = "Broken"
startup_timeout_secs = 10
tool_timeout_secs = 60

[transport]
type = "http"
url = "https://example.com/mcp"

[transport.headers]
Authorization = "do-not-print" trailing-invalid-content
"#,
    )
    .await
    .unwrap();

    let err = import_legacy_directory_once(database.pool(), &source)
        .await
        .unwrap_err();
    let mut rendered = err.to_string();
    let mut source = err.source();
    while let Some(err) = source {
        rendered.push_str(&err.to_string());
        source = err.source();
    }

    assert!(!rendered.contains("do-not-print"));
}

#[tokio::test]
async fn corrupted_stored_row_returns_typed_error() {
    let (_dir, database) = test_database().await;
    let mut connection = database.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        r"
        INSERT INTO mcp_servers (
            id, revision, display_name, transport_type, command_json, env_json,
            startup_timeout_secs, tool_timeout_secs
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ",
    )
    .bind("broken")
    .bind("not-a-revision")
    .bind("Broken")
    .bind("stdio")
    .bind(r#"["server"]"#)
    .bind("{}")
    .bind(10_i64)
    .bind(60_i64)
    .execute(&mut *connection)
    .await
    .unwrap();

    let err = McpServerStore::load(database.clone_pool())
        .await
        .unwrap_err();
    assert!(matches!(err, McpServerStoreError::StoredRevision { .. }));
}

#[tokio::test]
async fn store_debug_does_not_expose_transport_values() {
    let (_dir, database) = test_database().await;
    let store = McpServerStore::load(database.clone_pool()).await.unwrap();
    let mut secret_draft = draft("secret", "Secret");
    secret_draft.transport = McpTransport::Http {
        protocol: McpHttpProtocol::default(),
        url:      "https://example.com/mcp".to_string(),
        headers:  HashMap::from([("Authorization".to_string(), "do-not-print".to_string())]),
    };
    store.create(secret_draft).await.unwrap();

    let debug = format!("{store:?}");
    assert!(!debug.contains("do-not-print"));
}
