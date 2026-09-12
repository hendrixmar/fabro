use chrono::{DateTime, Duration, Utc};
pub use fabro_types::{OAuthConfig, OAuthCredential, OAuthTokens};

pub(crate) fn expires_at_from_now(expires_in: Option<u64>) -> DateTime<Utc> {
    let seconds = i64::try_from(expires_in.unwrap_or(3600)).unwrap_or(i64::MAX);
    Utc::now() + Duration::seconds(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(expires_at: DateTime<Utc>) -> OAuthCredential {
        OAuthCredential {
            tokens:     OAuthTokens {
                access_token: "access".to_string(),
                refresh_token: Some("refresh".to_string()),
                expires_at,
            },
            config:     OAuthConfig {
                auth_url:     "https://auth.openai.com".to_string(),
                token_url:    "https://auth.openai.com/oauth/token".to_string(),
                client_id:    "client".to_string(),
                scopes:       vec!["openid".to_string()],
                redirect_uri: Some("https://auth.openai.com/deviceauth/callback".to_string()),
                use_pkce:     true,
            },
            account_id: Some("acct_123".to_string()),
        }
    }

    #[test]
    fn round_trips_through_json() {
        let credential = fixture(Utc::now() + Duration::hours(1));
        let json = serde_json::to_string(&credential).unwrap();
        let parsed: OAuthCredential = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, credential);
    }

    #[test]
    fn needs_refresh_uses_five_minute_buffer() {
        assert!(fixture(Utc::now() + Duration::minutes(4)).needs_refresh());
        assert!(!fixture(Utc::now() + Duration::minutes(6)).needs_refresh());
    }
}
