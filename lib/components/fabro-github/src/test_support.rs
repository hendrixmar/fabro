use std::sync::Arc;

use crate::InstallationToken;
use crate::token_source::{InstallationTokenMinter as InnerMinter, InstallationTokenSource};

#[async_trait::async_trait]
pub trait InstallationTokenMinter: Send + Sync {
    async fn mint(&self) -> anyhow::Result<InstallationToken>;
}

struct TestMinterAdapter(Arc<dyn InstallationTokenMinter>);

#[async_trait::async_trait]
impl InnerMinter for TestMinterAdapter {
    async fn mint(&self) -> anyhow::Result<InstallationToken> {
        self.0.mint().await
    }
}

#[must_use]
pub fn installation_token_source(
    repo: impl Into<String>,
    minter: Arc<dyn InstallationTokenMinter>,
) -> Arc<InstallationTokenSource> {
    InstallationTokenSource::with_minter(repo.into(), Box::new(TestMinterAdapter(minter)))
}
