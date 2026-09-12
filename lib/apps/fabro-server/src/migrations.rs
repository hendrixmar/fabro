#[path = "../migrations/sqlite_activation_backup.rs"]
mod sqlite_activation_backup;
#[path = "../migrations/2026082301_sqlite_blob_activation.rs"]
mod sqlite_blob_activation;
#[path = "../migrations/2026082801_sqlite_run_history_activation.rs"]
mod sqlite_run_history_activation;

pub(crate) use sqlite_blob_activation::activate_blob_storage;
pub(crate) use sqlite_run_history_activation::activate_run_history;
