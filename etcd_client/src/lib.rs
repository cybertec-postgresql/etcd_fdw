//! Synchronous etcd v3 HTTP client using ureq

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use std::time::Duration;
use thiserror::Error;

pub use base64::engine::general_purpose::STANDARD as BASE64;

// Global ureq agent - must be static because ureq Agent is not Send/Sync but
// we need it to live across PostgreSQL statement executions
static UREQ_AGENT: LazyLock<ureq::Agent> = LazyLock::new(|| ureq::Agent::new());

#[derive(Error, Debug)]
pub enum EtcdError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("JSON error: {0}")]
    Json(String),
    #[error("etcd error: {0}")]
    Etcd(String),
}

/// Options for range queries
#[derive(Default, Clone)]
pub struct RangeOptions {
    pub range_end: Option<String>,
    pub limit: Option<u64>,
    pub revision: Option<i64>,
    pub serializable: bool,
    pub keys_only: bool,
    pub sort_order: Option<i32>,
    pub sort_target: Option<i32>,
    pub prefix: bool,
}

impl RangeOptions {
    pub fn with_range(mut self, range_end: String) -> Self {
        self.range_end = Some(range_end);
        self
    }
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
    pub fn with_revision(mut self, revision: i64) -> Self {
        self.revision = Some(revision);
        self
    }
    pub fn with_serializable(mut self) -> Self {
        self.serializable = true;
        self
    }
    pub fn with_keys_only(mut self) -> Self {
        self.keys_only = true;
        self
    }
    pub fn with_sort(mut self, target: i32, order: i32) -> Self {
        self.sort_target = Some(target);
        self.sort_order = Some(order);
        self
    }
    pub fn with_prefix(mut self) -> Self {
        self.prefix = true;
        self
    }
}

/// etcd v3 API key-value pair
#[derive(Clone, Debug)]
pub struct EtcdKeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl EtcdKeyValue {
    pub fn key(&self) -> &[u8] {
        &self.key
    }
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Synchronous etcd HTTP client using ureq
pub struct EtcdHttpClient {
    endpoint: String,
    timeout: Duration,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

impl EtcdHttpClient {
    pub fn new(endpoint: String, timeout: Duration) -> Self {
        // Add http:// prefix if no scheme is present
        let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint
        } else {
            format!("http://{}", endpoint)
        };
        Self {
            endpoint,
            timeout,
            username: None,
            password: None,
            token: None,
        }
    }

    pub fn with_auth(mut self, username: String, password: String) -> Self {
        self.username = Some(username);
        self.password = Some(password);
        self
    }

    fn base64_encode(input: &str) -> String {
        BASE64.encode(input.as_bytes())
    }

    fn base64_decode(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
        BASE64.decode(input)
    }

    /// Authenticate with etcd and get a token
    pub fn authenticate(&mut self) -> Result<(), EtcdError> {
        let (Some(user), Some(pass)) = (&self.username, &self.password) else {
            return Ok(()); // No auth configured
        };

        #[derive(Serialize)]
        struct AuthRequest {
            name: String,
            password: String,
        }

        #[derive(Deserialize)]
        struct AuthResponse {
            token: String,
        }

        let req = AuthRequest {
            name: user.clone(),
            password: pass.clone(),
        };

        let url = format!("{}/v3/auth/authenticate", self.endpoint);
        let resp = UREQ_AGENT
            .post(&url)
            .timeout(self.timeout)
            .send_json(&req)
            .map_err(|e| EtcdError::Http(e.to_string()))?;

        let auth_resp: AuthResponse = resp
            .into_json()
            .map_err(|e| EtcdError::Json(e.to_string()))?;

        self.token = Some(auth_resp.token);
        Ok(())
    }

    pub fn range(
        &mut self,
        key: &str,
        options: RangeOptions,
    ) -> Result<Vec<EtcdKeyValue>, EtcdError> {
        // Ensure we have a token if auth is required
        if self.token.is_none() && (self.username.is_some() || self.password.is_some()) {
            self.authenticate()?;
        }

        #[derive(Serialize)]
        struct RangeRequest {
            key: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            range_end: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            revision: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            serializable: Option<bool>,
            #[serde(skip_serializing_if = "Option::is_none")]
            keys_only: Option<bool>,
            #[serde(skip_serializing_if = "Option::is_none")]
            count_only: Option<bool>,
            #[serde(skip_serializing_if = "Option::is_none")]
            sort_order: Option<i32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            sort_target: Option<i32>,
        }

        #[derive(Deserialize)]
        struct RangeResponse {
            kvs: Option<Vec<KvPair>>,
            count: Option<serde_json::Value>,
        }

        #[derive(Deserialize)]
        struct KvPair {
            key: String,
            value: String,
        }

        let req = RangeRequest {
            key: Self::base64_encode(key),
            range_end: options.range_end.map(|s| Self::base64_encode(&s)),
            limit: options.limit,
            revision: options.revision,
            serializable: if options.serializable {
                Some(true)
            } else {
                None
            },
            keys_only: if options.keys_only { Some(true) } else { None },
            count_only: None,
            sort_order: options.sort_order,
            sort_target: options.sort_target,
        };

        let url = format!("{}/v3/kv/range", self.endpoint);
        let mut req_builder = UREQ_AGENT.post(&url).timeout(self.timeout);
        if let Some(t) = &self.token {
            req_builder = req_builder.set("Authorization", t);
        }
        let resp = req_builder
            .send_json(&req)
            .map_err(|e| EtcdError::Http(e.to_string()))?;

        let range_resp: RangeResponse = resp
            .into_json()
            .map_err(|e| EtcdError::Json(e.to_string()))?;

        let kvs = range_resp
            .kvs
            .unwrap_or_default()
            .into_iter()
            .map(|kv| EtcdKeyValue {
                key: Self::base64_decode(&kv.key).unwrap_or_default(),
                value: Self::base64_decode(&kv.value).unwrap_or_default(),
            })
            .collect();

        Ok(kvs)
    }

    pub fn put(&mut self, key: &str, value: &str) -> Result<(), EtcdError> {
        // Ensure we have a token if auth is required
        if self.token.is_none() && (self.username.is_some() || self.password.is_some()) {
            self.authenticate()?;
        }

        #[derive(Serialize)]
        struct PutRequest {
            key: String,
            value: String,
        }

        let req = PutRequest {
            key: Self::base64_encode(key),
            value: Self::base64_encode(value),
        };

        let url = format!("{}/v3/kv/put", self.endpoint);
        let mut req_builder = UREQ_AGENT.post(&url).timeout(self.timeout);
        if let Some(t) = &self.token {
            req_builder = req_builder.set("Authorization", t);
        }
        req_builder
            .send_json(&req)
            .map_err(|e| EtcdError::Http(e.to_string()))?;

        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<u64, EtcdError> {
        // Ensure we have a token if auth is required
        if self.token.is_none() && (self.username.is_some() || self.password.is_some()) {
            self.authenticate()?;
        }

        #[derive(Serialize)]
        struct DeleteRequest {
            key: String,
        }

        #[derive(Deserialize)]
        struct DeleteResponse {
            deleted: serde_json::Value,
        }

        let req = DeleteRequest {
            key: Self::base64_encode(key),
        };

        let url = format!("{}/v3/kv/deleterange", self.endpoint);
        let mut req_builder = UREQ_AGENT.post(&url).timeout(self.timeout);
        if let Some(t) = &self.token {
            req_builder = req_builder.set("Authorization", t);
        }
        let resp = req_builder
            .send_json(&req)
            .map_err(|e| EtcdError::Http(e.to_string()))?;

        let del_resp: DeleteResponse = resp
            .into_json()
            .map_err(|e| EtcdError::Json(e.to_string()))?;

        // Handle both string and number formats
        let deleted = match del_resp.deleted {
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
            serde_json::Value::String(s) => s.parse().unwrap_or(0),
            _ => 0,
        };

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_encode_decode() {
        let original = "hello world";
        let encoded = EtcdHttpClient::base64_encode(original);
        assert_eq!(encoded, "aGVsbG8gd29ybGQ=");
        let decoded = EtcdHttpClient::base64_decode(&encoded).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), original);
    }

    #[test]
    fn test_range_options_builder() {
        let opts = RangeOptions::default()
            .with_prefix()
            .with_limit(10)
            .with_revision(5);

        assert!(opts.prefix);
        assert_eq!(opts.limit, Some(10));
        assert_eq!(opts.revision, Some(5));
    }
}
