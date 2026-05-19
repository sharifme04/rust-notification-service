// Auth helpers — wired into request paths in a later phase.
#![allow(dead_code)]

use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

pub fn verify(token: &str, secret: &str) -> jsonwebtoken::errors::Result<Claims> {
    let key = DecodingKey::from_secret(secret.as_bytes());
    let validation = Validation::default();
    decode::<Claims>(token, &key, &validation).map(|d| d.claims)
}
