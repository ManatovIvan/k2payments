use std::collections::HashSet;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminResource {
    Ready,
    Status,
    Transaction,
    Reload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    Disabled,
    LegacyBearer,
    JwtHs256,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub jwt_hs256_secret: Option<String>,
    pub jwt_issuer: Option<String>,
    pub jwt_audience: Option<String>,
    pub ready_roles: Vec<String>,
    pub status_roles: Vec<String>,
    pub tx_roles: Vec<String>,
    pub reload_roles: Vec<String>,
    pub require_mtls_subject: bool,
    pub mtls_subject_header: String,
    pub mtls_allowed_subjects: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::LegacyBearer,
            jwt_hs256_secret: None,
            jwt_issuer: None,
            jwt_audience: None,
            ready_roles: Vec::new(),
            status_roles: Vec::new(),
            tx_roles: Vec::new(),
            reload_roles: Vec::new(),
            require_mtls_subject: false,
            mtls_subject_header: "x-client-cert-subject".to_string(),
            mtls_allowed_subjects: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing bearer token")]
    MissingBearer,
    #[error("invalid bearer token")]
    InvalidBearer,
    #[error("forbidden")]
    Forbidden,
    #[error("missing mTLS subject")]
    MissingMtlsSubject,
    #[error("untrusted mTLS subject")]
    UntrustedMtlsSubject,
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    scope: Option<String>,
}

pub fn authorize_request(
    config: &AuthConfig,
    resource: AdminResource,
    bearer_header: Option<&str>,
    mtls_subject: Option<&str>,
) -> Result<(), AuthError> {
    verify_mtls_subject(config, mtls_subject)?;

    match config.mode {
        AuthMode::Disabled => Ok(()),
        AuthMode::LegacyBearer => authorize_legacy(resource, bearer_header),
        AuthMode::JwtHs256 => authorize_jwt(config, resource, bearer_header),
    }
}

fn authorize_legacy(resource: AdminResource, bearer_header: Option<&str>) -> Result<(), AuthError> {
    let token = parse_bearer_token(bearer_header).ok_or(AuthError::MissingBearer)?;
    if (resource == AdminResource::Transaction || resource == AdminResource::Reload)
        && token == "readonly"
    {
        return Err(AuthError::Forbidden);
    }
    Ok(())
}

fn authorize_jwt(
    config: &AuthConfig,
    resource: AdminResource,
    bearer_header: Option<&str>,
) -> Result<(), AuthError> {
    let token = parse_bearer_token(bearer_header).ok_or(AuthError::MissingBearer)?;
    let secret = config
        .jwt_hs256_secret
        .as_deref()
        .ok_or(AuthError::InvalidBearer)?;

    let mut validation = Validation::new(Algorithm::HS256);
    if let Some(iss) = &config.jwt_issuer {
        validation.set_issuer(&[iss]);
    }
    if let Some(aud) = &config.jwt_audience {
        validation.set_audience(&[aud]);
    }

    let data = decode::<JwtClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|_| AuthError::InvalidBearer)?;

    let required_roles = required_roles(config, resource);
    if required_roles.is_empty() {
        return Ok(());
    }

    let mut principal_roles: HashSet<String> = data.claims.roles.into_iter().collect();
    if let Some(role) = data.claims.role {
        principal_roles.insert(role);
    }
    if let Some(scope) = data.claims.scope {
        for entry in scope.split_whitespace() {
            if !entry.is_empty() {
                principal_roles.insert(entry.to_string());
            }
        }
    }

    if required_roles
        .iter()
        .any(|required| principal_roles.contains(required))
    {
        return Ok(());
    }

    Err(AuthError::Forbidden)
}

fn required_roles(config: &AuthConfig, resource: AdminResource) -> &[String] {
    match resource {
        AdminResource::Ready => &config.ready_roles,
        AdminResource::Status => &config.status_roles,
        AdminResource::Transaction => &config.tx_roles,
        AdminResource::Reload => &config.reload_roles,
    }
}

fn verify_mtls_subject(config: &AuthConfig, mtls_subject: Option<&str>) -> Result<(), AuthError> {
    if !config.require_mtls_subject {
        return Ok(());
    }

    let subject = mtls_subject
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(AuthError::MissingMtlsSubject)?;

    if config.mtls_allowed_subjects.is_empty() {
        return Ok(());
    }

    if config
        .mtls_allowed_subjects
        .iter()
        .any(|allowed| allowed == subject)
    {
        return Ok(());
    }

    Err(AuthError::UntrustedMtlsSubject)
}

pub fn parse_bearer_token(header: Option<&str>) -> Option<&str> {
    let value = header?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde::Serialize;

    use super::{authorize_request, AdminResource, AuthConfig, AuthError, AuthMode};

    #[derive(Debug, Serialize)]
    struct Claims {
        sub: String,
        exp: usize,
        roles: Vec<String>,
    }

    #[test]
    fn jwt_mode_requires_expected_roles() {
        let cfg = AuthConfig {
            mode: AuthMode::JwtHs256,
            jwt_hs256_secret: Some("test-secret".to_string()),
            tx_roles: vec!["admin.tx.read".to_string()],
            ..AuthConfig::default()
        };
        let claims = Claims {
            sub: "operator".to_string(),
            exp: 4_102_444_800,
            roles: vec!["admin.tx.read".to_string()],
        };
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret("test-secret".as_bytes()),
        )
        .expect("token should encode");
        let header = format!("Bearer {token}");

        let result = authorize_request(
            &cfg,
            AdminResource::Transaction,
            Some(header.as_str()),
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn jwt_mode_denies_missing_role() {
        let cfg = AuthConfig {
            mode: AuthMode::JwtHs256,
            jwt_hs256_secret: Some("test-secret".to_string()),
            tx_roles: vec!["admin.tx.read".to_string()],
            ..AuthConfig::default()
        };
        let claims = Claims {
            sub: "readonly".to_string(),
            exp: 4_102_444_800,
            roles: vec!["admin.read".to_string()],
        };
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret("test-secret".as_bytes()),
        )
        .expect("token should encode");
        let header = format!("Bearer {token}");

        let result = authorize_request(
            &cfg,
            AdminResource::Transaction,
            Some(header.as_str()),
            None,
        );
        assert!(matches!(result, Err(AuthError::Forbidden)));
    }

    #[test]
    fn mtls_subject_is_enforced_when_enabled() {
        let cfg = AuthConfig {
            mode: AuthMode::Disabled,
            require_mtls_subject: true,
            mtls_allowed_subjects: vec!["CN=trusted-client".to_string()],
            ..AuthConfig::default()
        };

        let denied = authorize_request(&cfg, AdminResource::Ready, None, Some("CN=other-client"));
        assert!(matches!(denied, Err(AuthError::UntrustedMtlsSubject)));

        let accepted =
            authorize_request(&cfg, AdminResource::Ready, None, Some("CN=trusted-client"));
        assert!(accepted.is_ok());
    }
}
