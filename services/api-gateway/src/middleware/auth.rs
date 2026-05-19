use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
};
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    /// Subject — user id (UUID string).
    pub sub: String,
    pub exp: usize,
}

/// Axum extractor: reads `Authorization: Bearer <token>` and validates it.
/// Returns 401 if the header is missing or the token is invalid.
pub struct AuthUser(pub Claims);

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or((
                StatusCode::UNAUTHORIZED,
                "missing or invalid Authorization header",
            ))?;

        let key = DecodingKey::from_secret(state.jwt_secret.as_bytes());
        let claims = decode::<Claims>(token, &key, &Validation::default())
            .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid token"))?
            .claims;

        Ok(AuthUser(claims))
    }
}
