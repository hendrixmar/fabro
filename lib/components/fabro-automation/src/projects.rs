//! Native projects: an existing GitHub repository connected to this server.
//!
//! A project is the native grouping for feature intake and for concrete
//! per-project automation instances. Connecting a repository performs GitHub
//! reads and local persistence only: no clone, Plane mutation, CI activation,
//! or deployment happens on this path.
//!
//! Repository identity is the immutable numeric GitHub repository ID, stored
//! as a decimal string because it exceeds the exact range of a JavaScript
//! number. The `owner/repo` slug and default branch are cached metadata the
//! server re-reads from GitHub; they are never caller-supplied authority.

use std::fmt;
use std::str::FromStr;

use fabro_db::DbPool;
use fabro_types::GitHubRepositorySlug;
use serde::de::Error as _;
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest, Sha256};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row as _, Sqlite, Transaction};

use crate::id::is_valid_id;

/// Longest accepted decimal form of a GitHub repository id (u64).
const GITHUB_REPOSITORY_ID_MAX_LEN: usize = 20;

/// Shared projection for loading projects. A macro rather than a `const`
/// because sqlx requires `&'static str` SQL.
macro_rules! select_projects_sql {
    ($suffix:expr) => {
        concat!(
            "SELECT
                id,
                revision,
                name,
                github_repository_id,
                repository,
                default_branch,
                intake_binding_id
            FROM projects ",
            $suffix
        )
    };
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProjectId(String);

impl ProjectId {
    pub fn new(value: impl Into<String>) -> Result<Self, ProjectValidationError> {
        let value = value.into();
        if is_valid_id(&value, false) {
            Ok(Self(value))
        } else {
            Err(ProjectValidationError::InvalidProjectId { value })
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ProjectId {
    type Err = ProjectValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for ProjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

/// Immutable numeric GitHub repository identity, carried as a decimal string.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GithubRepositoryId(String);

impl GithubRepositoryId {
    pub fn new(value: impl Into<String>) -> Result<Self, ProjectValidationError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= GITHUB_REPOSITORY_ID_MAX_LEN
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && (value == "0" || !value.starts_with('0'));
        if valid {
            Ok(Self(value))
        } else {
            Err(ProjectValidationError::InvalidGithubRepositoryId { value })
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The identity as a number, when it fits. Used only for comparisons and
    /// diagnostics; the decimal string stays the stored form.
    #[must_use]
    pub fn to_u64(&self) -> Option<u64> {
        self.0.parse().ok()
    }
}

impl fmt::Display for GithubRepositoryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GithubRepositoryId {
    type Err = ProjectValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for GithubRepositoryId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GithubRepositoryId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

/// Optimistic-concurrency revision of the mutable project row.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProjectRevision(String);

impl ProjectRevision {
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(hex::encode(Sha256::digest(bytes)))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProjectRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ProjectRevision {
    type Err = ProjectRevisionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let valid = value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
        if valid {
            Ok(Self(value.to_string()))
        } else {
            Err(ProjectRevisionParseError(value.to_string()))
        }
    }
}

impl Serialize for ProjectRevision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProjectRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRevisionParseError(String);

impl fmt::Display for ProjectRevisionParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "project revision {:?} must be 64 lowercase hexadecimal characters",
            self.0
        )
    }
}

impl std::error::Error for ProjectRevisionParseError {}

/// A GitHub repository connected to this server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Project {
    pub id:                   ProjectId,
    pub revision:             ProjectRevision,
    pub name:                 String,
    /// Immutable numeric GitHub repository ID, rendered as a decimal string.
    pub github_repository_id: GithubRepositoryId,
    /// Canonical `owner/repo` slug as last read from GitHub.
    pub repository:           String,
    /// Default branch as last read from GitHub.
    pub default_branch:       String,
    /// Registered feature-intake binding, when feature intake is connected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intake_binding_id:    Option<String>,
}

impl Project {
    /// Field order is the canonical revision input; keep it stable so an
    /// unchanged project keeps its revision across restarts.
    fn canonical_bytes(
        name: &str,
        github_repository_id: &str,
        repository: &str,
        default_branch: &str,
        intake_binding_id: Option<&str>,
    ) -> Vec<u8> {
        #[derive(Serialize)]
        struct Canonical<'a> {
            name:                 &'a str,
            github_repository_id: &'a str,
            repository:           &'a str,
            default_branch:       &'a str,
            intake_binding_id:    Option<&'a str>,
        }
        serde_json::to_vec(&Canonical {
            name,
            github_repository_id,
            repository,
            default_branch,
            intake_binding_id,
        })
        .expect("project revision input is JSON-serializable")
    }

    #[must_use]
    pub fn revision_for(
        name: &str,
        github_repository_id: &str,
        repository: &str,
        default_branch: &str,
        intake_binding_id: Option<&str>,
    ) -> ProjectRevision {
        ProjectRevision::from_bytes(&Self::canonical_bytes(
            name,
            github_repository_id,
            repository,
            default_branch,
            intake_binding_id,
        ))
    }
}

impl From<SqliteProjectRow> for Project {
    fn from(row: SqliteProjectRow) -> Self {
        Self {
            id:                   row.id,
            revision:             row.revision,
            name:                 row.name,
            github_repository_id: row.github_repository_id,
            repository:           row.repository,
            default_branch:       row.default_branch,
            intake_binding_id:    row.intake_binding_id,
        }
    }
}

struct SqliteProjectRow {
    id:                   ProjectId,
    revision:             ProjectRevision,
    name:                 String,
    github_repository_id: GithubRepositoryId,
    repository:           String,
    default_branch:       String,
    intake_binding_id:    Option<String>,
}

impl SqliteProjectRow {
    fn from_row(row: &SqliteRow) -> Result<Self, ProjectStoreError> {
        let raw_id: String = row.try_get("id")?;
        let id = ProjectId::new(raw_id).map_err(|source| ProjectStoreError::StoredRow {
            id:     None,
            reason: "invalid project id".to_string(),
            source: Some(source),
        })?;
        let parse = |reason: &str| ProjectStoreError::StoredRow {
            id:     Some(id.clone()),
            reason: reason.to_string(),
            source: None,
        };

        let raw_revision: String = row.try_get("revision")?;
        let revision = raw_revision
            .parse::<ProjectRevision>()
            .map_err(|_| parse("invalid project revision"))?;
        let raw_github_id: String = row.try_get("github_repository_id")?;
        let github_repository_id = GithubRepositoryId::new(raw_github_id)
            .map_err(|_source| parse("invalid GitHub repository id"))?;
        let repository: String = row.try_get("repository")?;
        let slug = GitHubRepositorySlug::try_new(&repository)
            .ok_or_else(|| parse("invalid repository slug"))?;
        let default_branch: String = row.try_get("default_branch")?;
        validate_default_branch(&default_branch).map_err(|_| parse("invalid default branch"))?;
        let name: String = row.try_get("name")?;
        validate_name(&name).map_err(|_| parse("invalid project name"))?;
        let intake_binding_id: Option<String> = row.try_get("intake_binding_id")?;
        if let Some(binding) = intake_binding_id.as_deref() {
            validate_intake_binding(binding).map_err(|_| parse("invalid intake binding id"))?;
        }

        Ok(Self {
            id,
            revision,
            name,
            github_repository_id,
            repository: slug.to_string(),
            default_branch,
            intake_binding_id,
        })
    }
}

/// Server-resolved creation input. Every field except `id` and `name` is
/// canonical GitHub metadata; callers may not supply permission or
/// default-branch claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDraft {
    pub id:                   ProjectId,
    pub name:                 String,
    pub github_repository_id: GithubRepositoryId,
    pub repository:           String,
    pub default_branch:       String,
}

/// Rename-only replacement. Repository identity and intake binding are not
/// mutable through the generic project update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReplace {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ProjectStore {
    pool: DbPool,
}

impl ProjectStore {
    #[must_use]
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn list(&self) -> Result<Vec<Project>, ProjectStoreError> {
        let rows = sqlx::query(select_projects_sql!("ORDER BY id"))
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(SqliteProjectRow::from_row)
            .map(|row| row.map(Project::from))
            .collect()
    }

    pub async fn get(&self, id: &ProjectId) -> Result<Option<Project>, ProjectStoreError> {
        let row = sqlx::query(select_projects_sql!("WHERE id = ?"))
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref()
            .map(SqliteProjectRow::from_row)
            .transpose()
            .map(|row| row.map(Project::from))
    }

    pub async fn find_by_github_id(
        &self,
        github_repository_id: &GithubRepositoryId,
    ) -> Result<Option<Project>, ProjectStoreError> {
        let row = sqlx::query(select_projects_sql!("WHERE github_repository_id = ?"))
            .bind(github_repository_id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref()
            .map(SqliteProjectRow::from_row)
            .transpose()
            .map(|row| row.map(Project::from))
    }

    pub async fn find_by_repository(
        &self,
        repository: &str,
    ) -> Result<Option<Project>, ProjectStoreError> {
        let slug = GitHubRepositorySlug::try_new(repository).ok_or_else(|| {
            ProjectValidationError::InvalidRepository {
                value: repository.to_string(),
            }
        })?;
        let row = sqlx::query(select_projects_sql!("WHERE repository_key = ?"))
            .bind(slug.to_string().to_lowercase())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref()
            .map(SqliteProjectRow::from_row)
            .transpose()
            .map(|row| row.map(Project::from))
    }

    /// Insert one project. A duplicate repository (numeric id or slug) is
    /// reported with the ID of the project that already owns it, whether the
    /// conflict is pre-existing or produced by a concurrent create.
    pub async fn create(&self, draft: ProjectDraft) -> Result<Project, ProjectStoreError> {
        validate_name(&draft.name)?;
        validate_default_branch(&draft.default_branch)?;
        let slug = GitHubRepositorySlug::try_new(&draft.repository).ok_or_else(|| {
            ProjectValidationError::InvalidRepository {
                value: draft.repository.clone(),
            }
        })?;
        let repository = slug.to_string();
        let repository_key = repository.to_lowercase();
        let project = Project {
            id: draft.id,
            revision: Project::revision_for(
                &draft.name,
                draft.github_repository_id.as_str(),
                &repository,
                &draft.default_branch,
                None,
            ),
            name: draft.name,
            github_repository_id: draft.github_repository_id,
            repository,
            default_branch: draft.default_branch,
            intake_binding_id: None,
        };

        let mut transaction = self.pool.begin().await?;
        if let Some(conflict) = find_repository_conflict(&mut transaction, &project).await? {
            return Err(conflict.into_error(&project.repository));
        }
        let result = sqlx::query(
            r"
            INSERT INTO projects (
                id, revision, name, github_repository_id, repository, repository_key,
                default_branch, intake_binding_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            ",
        )
        .bind(project.id.as_str())
        .bind(project.revision.as_str())
        .bind(&project.name)
        .bind(project.github_repository_id.as_str())
        .bind(&project.repository)
        .bind(&repository_key)
        .bind(&project.default_branch)
        .execute(&mut *transaction)
        .await;

        match result {
            Ok(_) => {
                transaction.commit().await?;
                Ok(project)
            }
            Err(err) if is_unique_violation(&err) => {
                transaction.rollback().await?;
                if self.get(&project.id).await?.is_some() {
                    return Err(ProjectStoreError::AlreadyExists { id: project.id });
                }
                Err(ProjectStoreError::RepositoryConflict {
                    repository:          project.repository,
                    existing_project_id: None,
                })
            }
            Err(err) => Err(ProjectStoreError::Db { source: err }),
        }
    }

    /// Rename a project without touching its repository identity or binding.
    pub async fn rename(
        &self,
        id: &ProjectId,
        expected: &ProjectRevision,
        replace: ProjectReplace,
    ) -> Result<Project, ProjectStoreError> {
        validate_name(&replace.name)?;
        let mut transaction = self.pool.begin().await?;
        let current = load_for_update(&mut transaction, id).await?;
        let current = current.ok_or_else(|| ProjectStoreError::NotFound { id: id.clone() })?;
        if &current.revision != expected {
            return Err(ProjectStoreError::StaleRevision {
                id:       id.clone(),
                expected: expected.clone(),
                actual:   current.revision,
            });
        }
        let revision = Project::revision_for(
            &replace.name,
            current.github_repository_id.as_str(),
            &current.repository,
            &current.default_branch,
            current.intake_binding_id.as_deref(),
        );
        let result =
            sqlx::query("UPDATE projects SET revision = ?, name = ? WHERE id = ? AND revision = ?")
                .bind(revision.as_str())
                .bind(&replace.name)
                .bind(id.as_str())
                .bind(expected.as_str())
                .execute(&mut *transaction)
                .await?;
        if result.rows_affected() == 0 {
            return Err(ProjectStoreError::StaleRevision {
                id:       id.clone(),
                expected: expected.clone(),
                actual:   current.revision,
            });
        }
        transaction.commit().await?;
        Ok(Project {
            revision,
            name: replace.name,
            ..current
        })
    }

    /// Attach or detach the registered feature-intake binding. The binding is
    /// a registry project id; it is unique across native projects.
    pub async fn set_intake_binding(
        &self,
        id: &ProjectId,
        expected: &ProjectRevision,
        binding_id: Option<&str>,
    ) -> Result<Project, ProjectStoreError> {
        if let Some(binding_id) = binding_id {
            validate_intake_binding(binding_id)?;
        }
        let mut transaction = self.pool.begin().await?;
        let current = load_for_update(&mut transaction, id).await?;
        let current = current.ok_or_else(|| ProjectStoreError::NotFound { id: id.clone() })?;
        if &current.revision != expected {
            return Err(ProjectStoreError::StaleRevision {
                id:       id.clone(),
                expected: expected.clone(),
                actual:   current.revision,
            });
        }
        let revision = Project::revision_for(
            &current.name,
            current.github_repository_id.as_str(),
            &current.repository,
            &current.default_branch,
            binding_id,
        );
        let result = sqlx::query(
            "UPDATE projects SET revision = ?, intake_binding_id = ? WHERE id = ? AND revision = ?",
        )
        .bind(revision.as_str())
        .bind(binding_id)
        .bind(id.as_str())
        .bind(expected.as_str())
        .execute(&mut *transaction)
        .await;
        match result {
            Ok(result) if result.rows_affected() == 0 => Err(ProjectStoreError::StaleRevision {
                id:       id.clone(),
                expected: expected.clone(),
                actual:   current.revision,
            }),
            Ok(_) => {
                transaction.commit().await?;
                Ok(Project {
                    revision,
                    intake_binding_id: binding_id.map(ToString::to_string),
                    ..current
                })
            }
            Err(err) if is_unique_violation(&err) => {
                transaction.rollback().await?;
                let existing = match binding_id {
                    Some(binding_id) => self.find_by_intake_binding(binding_id).await?,
                    None => None,
                };
                Err(ProjectStoreError::IntakeBindingConflict {
                    binding_id:          binding_id.map(ToString::to_string),
                    existing_project_id: existing.map(|project| project.id),
                })
            }
            Err(err) => Err(ProjectStoreError::Db { source: err }),
        }
    }

    pub async fn find_by_intake_binding(
        &self,
        binding_id: &str,
    ) -> Result<Option<Project>, ProjectStoreError> {
        let row = sqlx::query(select_projects_sql!("WHERE intake_binding_id = ?"))
            .bind(binding_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref()
            .map(SqliteProjectRow::from_row)
            .transpose()
            .map(|row| row.map(Project::from))
    }
}

enum RepositoryConflict {
    GithubId(ProjectId),
    Slug(ProjectId),
}

impl RepositoryConflict {
    fn into_error(self, repository: &str) -> ProjectStoreError {
        ProjectStoreError::RepositoryConflict {
            repository:          repository.to_string(),
            existing_project_id: Some(match self {
                Self::GithubId(id) | Self::Slug(id) => id,
            }),
        }
    }
}

async fn find_repository_conflict(
    transaction: &mut Transaction<'_, Sqlite>,
    project: &Project,
) -> Result<Option<RepositoryConflict>, ProjectStoreError> {
    let row = sqlx::query(
        "SELECT id, github_repository_id, repository_key FROM projects
         WHERE github_repository_id = ? OR repository_key = ?",
    )
    .bind(project.github_repository_id.as_str())
    .bind(project.repository.to_lowercase())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let existing: String = row.try_get("id")?;
    let existing = ProjectId::new(existing).map_err(|source| ProjectStoreError::StoredRow {
        id:     None,
        reason: "invalid project id".to_string(),
        source: Some(source),
    })?;
    let matched_github_id: String = row.try_get("github_repository_id")?;
    if matched_github_id == project.github_repository_id.as_str() {
        Ok(Some(RepositoryConflict::GithubId(existing)))
    } else {
        Ok(Some(RepositoryConflict::Slug(existing)))
    }
}

async fn load_for_update(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &ProjectId,
) -> Result<Option<Project>, ProjectStoreError> {
    let row = sqlx::query(select_projects_sql!("WHERE id = ?"))
        .bind(id.as_str())
        .fetch_optional(&mut **transaction)
        .await?;
    row.as_ref()
        .map(SqliteProjectRow::from_row)
        .transpose()
        .map(|row| row.map(Project::from))
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            let message = db_err.message();
            matches!(db_err.code().as_deref(), Some("1555" | "2067"))
                || message.contains("UNIQUE constraint failed")
        }
        _ => false,
    }
}

fn validate_name(name: &str) -> Result<(), ProjectValidationError> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.len() > 200 || trimmed.chars().any(char::is_control) {
        return Err(ProjectValidationError::InvalidName {
            value: name.to_string(),
        });
    }
    Ok(())
}

fn validate_default_branch(branch: &str) -> Result<(), ProjectValidationError> {
    let trimmed = branch.trim();
    let valid = !trimmed.is_empty()
        && trimmed == branch
        && branch.len() <= 255
        && !branch.starts_with(['-', '/', '.'])
        && !branch.ends_with(['/', '.'])
        && !branch.contains("..")
        && !branch.contains("@{")
        && !branch.contains("//")
        && !branch.bytes().any(|byte| {
            byte.is_ascii_control()
                || matches!(byte, b' ' | b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        });
    if valid {
        Ok(())
    } else {
        Err(ProjectValidationError::InvalidDefaultBranch {
            value: branch.to_string(),
        })
    }
}

fn validate_intake_binding(binding_id: &str) -> Result<(), ProjectValidationError> {
    let trimmed = binding_id.trim();
    if trimmed.is_empty() || trimmed.len() > 63 || trimmed != binding_id {
        return Err(ProjectValidationError::InvalidIntakeBinding {
            value: binding_id.to_string(),
        });
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ProjectValidationError {
    #[error("project id {value:?} must match [a-z0-9][a-z0-9-]{{0,62}}")]
    InvalidProjectId { value: String },
    #[error("GitHub repository id {value:?} must be a canonical decimal string")]
    InvalidGithubRepositoryId { value: String },
    #[error("project name {value:?} must be non-empty and at most 200 characters")]
    InvalidName { value: String },
    #[error("project repository {value:?} must be a GitHub owner/repo slug")]
    InvalidRepository { value: String },
    #[error("project default branch {value:?} is not a valid Git branch name")]
    InvalidDefaultBranch { value: String },
    #[error("project intake binding {value:?} is not a valid binding id")]
    InvalidIntakeBinding { value: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ProjectStoreError {
    #[error("project not found: {id}")]
    NotFound { id: ProjectId },
    #[error("project already exists: {id}")]
    AlreadyExists { id: ProjectId },
    #[error(
        "repository {repository} is already connected as project {existing_project_id}",
        existing_project_id = existing_project_id.as_ref().map_or("<unknown>", ProjectId::as_str)
    )]
    RepositoryConflict {
        repository:          String,
        existing_project_id: Option<ProjectId>,
    },
    #[error("project revision is stale for {id}: expected {expected}, actual {actual}")]
    StaleRevision {
        id:       ProjectId,
        expected: ProjectRevision,
        actual:   ProjectRevision,
    },
    #[error(
        "intake binding {binding_id:?} is already attached to project {existing_project_id}",
        binding_id = binding_id.as_deref().unwrap_or("<none>"),
        existing_project_id = existing_project_id.as_ref().map_or("<unknown>", ProjectId::as_str)
    )]
    IntakeBindingConflict {
        binding_id:          Option<String>,
        existing_project_id: Option<ProjectId>,
    },
    #[error("project validation failed")]
    Validation {
        #[from]
        source: ProjectValidationError,
    },
    #[error("stored project row is invalid{}: {reason}", id.as_ref().map_or(String::new(), |id| format!(" for {id}")))]
    StoredRow {
        id:     Option<ProjectId>,
        reason: String,
        #[source]
        source: Option<ProjectValidationError>,
    },
    #[error("database error")]
    Db {
        #[from]
        source: sqlx::Error,
    },
}

impl ProjectStoreError {
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NotFound { .. } => "not_found",
            Self::AlreadyExists { .. } => "already_exists",
            Self::RepositoryConflict { .. } => "repository_conflict",
            Self::StaleRevision { .. } => "stale_revision",
            Self::IntakeBindingConflict { .. } => "intake_binding_conflict",
            Self::Validation { .. } => "validation",
            Self::StoredRow { .. } => "stored_row",
            Self::Db { .. } => "db",
        }
    }
}

#[cfg(test)]
mod tests {
    use fabro_db::Database;
    use tempfile::TempDir;

    use super::{
        GithubRepositoryId, Project, ProjectDraft, ProjectId, ProjectReplace, ProjectStore,
        ProjectStoreError,
    };

    async fn store() -> (TempDir, ProjectStore) {
        let dir = TempDir::new().expect("temp dir");
        let database = Database::connect(dir.path().join("fabro.sqlite"))
            .await
            .expect("database");
        database.migrate().await.expect("migrate");
        (dir, ProjectStore::new(database.clone_pool()))
    }

    fn draft(id: &str, repository: &str, github_id: &str) -> ProjectDraft {
        ProjectDraft {
            id:                   ProjectId::new(id).unwrap(),
            name:                 format!("Project {id}"),
            github_repository_id: GithubRepositoryId::new(github_id).unwrap(),
            repository:           repository.to_string(),
            default_branch:       "main".to_string(),
        }
    }

    #[tokio::test]
    async fn create_is_unique_by_repository_id_and_case_insensitive_slug() {
        let (_dir, store) = store().await;
        let created = store
            .create(draft("tierrapay", "artesanos-digitales/tierrapay", "1234"))
            .await
            .expect("create");

        let same_id = store
            .create(draft("other", "artesanos-digitales/other", "1234"))
            .await
            .expect_err("same GitHub id must conflict");
        assert!(matches!(
            same_id,
            ProjectStoreError::RepositoryConflict {
                existing_project_id: Some(existing),
                ..
            } if existing == created.id
        ));

        let same_slug = store
            .create(draft(
                "tierrapay-two",
                "Artesanos-Digitales/TierraPay",
                "5678",
            ))
            .await
            .expect_err("case-varied slug must conflict");
        assert!(matches!(
            same_slug,
            ProjectStoreError::RepositoryConflict {
                existing_project_id: Some(existing),
                ..
            } if existing == created.id
        ));

        let duplicate_id = store
            .create(draft("tierrapay", "artesanos-digitales/other", "9999"))
            .await
            .expect_err("duplicate project id must conflict");
        assert!(matches!(
            duplicate_id,
            ProjectStoreError::AlreadyExists { .. }
        ));

        assert_eq!(store.list().await.expect("list").len(), 1);
    }

    #[tokio::test]
    async fn rename_preserves_identity_and_requires_current_revision() {
        let (_dir, store) = store().await;
        let created = store
            .create(draft("tierrapay", "artesanos-digitales/tierrapay", "1234"))
            .await
            .expect("create");

        let renamed = store
            .rename(&created.id, &created.revision, ProjectReplace {
                name: "TierraPay".to_string(),
            })
            .await
            .expect("rename");
        assert_eq!(renamed.name, "TierraPay");
        assert_eq!(renamed.repository, created.repository);
        assert_ne!(renamed.revision, created.revision);

        let stale = store
            .rename(&created.id, &created.revision, ProjectReplace {
                name: "Stale".to_string(),
            })
            .await
            .expect_err("stale revision must fail");
        assert!(matches!(stale, ProjectStoreError::StaleRevision { .. }));
        assert_eq!(
            store.get(&created.id).await.expect("get").unwrap().name,
            "TierraPay"
        );
    }

    #[tokio::test]
    async fn intake_binding_is_unique_and_revision_checked() {
        let (_dir, store) = store().await;
        let first = store
            .create(draft("tierrapay", "artesanos-digitales/tierrapay", "1234"))
            .await
            .expect("create");
        let second = store
            .create(draft("mafeva", "artesanos-digitales/mafeva", "5678"))
            .await
            .expect("create");

        let bound = store
            .set_intake_binding(&first.id, &first.revision, Some("tierrapay"))
            .await
            .expect("bind");
        assert_eq!(bound.intake_binding_id.as_deref(), Some("tierrapay"));

        let conflict = store
            .set_intake_binding(&second.id, &second.revision, Some("tierrapay"))
            .await
            .expect_err("duplicate binding must conflict");
        assert!(matches!(
            conflict,
            ProjectStoreError::IntakeBindingConflict {
                existing_project_id: Some(existing),
                ..
            } if existing == first.id
        ));

        let stale = store
            .set_intake_binding(&first.id, &first.revision, None)
            .await
            .expect_err("stale revision must fail");
        assert!(matches!(stale, ProjectStoreError::StaleRevision { .. }));
        assert!(
            store
                .find_by_intake_binding("tierrapay")
                .await
                .expect("find")
                .is_some()
        );
    }

    #[test]
    fn revision_is_stable_for_unchanged_fields() {
        let left = Project::revision_for("a", "1", "o/r", "main", None);
        let right = Project::revision_for("a", "1", "o/r", "main", None);
        let other = Project::revision_for("a", "1", "o/r", "main", Some("b"));
        assert_eq!(left, right);
        assert_ne!(left, other);
    }
}
