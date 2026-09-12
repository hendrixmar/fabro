//! `sqlx::migrate!` embeds every file under `migrations/` at compile time,
//! but on stable Rust the macro cannot tell Cargo about the directory. Without
//! this hint a newly added migration file does not recompile the crate, so a
//! stale build silently ships without it.

fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
