use etcd_client::{Client, ConnectOptions, TlsOptions, Identity, Certificate, Error, DeleteOptions, GetOptions, KeyValue, PutOptions};
use std::time::Duration;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use supabase_wrappers::prelude::*;
use thiserror::Error;

pgrx::pg_module_magic!();

#[wrappers_fdw(
    version = "0.0.1",
    author = "Cybertec PostgreSQL International GmbH",
    error_type = "EtcdFdwError"
)]
pub(crate) struct EtcdFdw {
    client: Client,
    rt: Runtime,
    prefix: String,
    fetch_results: Vec<KeyValue>,
    fetch_key: bool,
    fetch_value: bool,
}
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub servername: Option<String>,
}

impl EtcdConfig {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            username: None,
            password: None,
            servername: None,
        }
    }
}


#[derive(Error, Debug)]
pub enum EtcdFdwError {
    #[error("Failed to fetch from etcd: {0}")]
    FetchError(String),

    #[error("Failed to send update to etcd: {0}")]
    UpdateError(String),

    #[error("Failed to connect to client: {0}")]
    ClientConnectionError(String),

    #[error("No connection string option was specified. Specify it with connstr")]
    NoConnStr(()),

    #[error("KeyFile and CertFile must both be present.")]
    CertKeyMismatch(()),

    #[error("Username and Password must both be specified.")]
    UserPassMismatch(()),

    #[error("Column {0} is not contained in the input dataset")]
    MissingColumn(String),

    #[error("Key {0} already exists in etcd. No duplicates allowed")]
    KeyAlreadyExists(String),

    #[error("Key {0} doesn't exist in etcd")]
    KeyDoesntExist(String),
}

impl From<EtcdFdwError> for ErrorReport {
    fn from(value: EtcdFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{}", value), "")
    }
}

/// Check weather dependent options exits
/// i.e username & pass, cert & key
fn require_pair<T>(
    a: &Option<T>,
    b: &Option<T>,
    err: EtcdFdwError,
) -> Result<(), EtcdFdwError> {
    match (a, b) {
        (Some(_), None) | (None, Some(_)) => Err(err),
        _ => Ok(()),
    }
}


/// Use this to connect to etcd.
/// Parse the certs/key paths and read them as bytes
/// Sets the `TlsOptions` if available to support sll connection
pub async fn connect_etcd(config: EtcdConfig) -> Result<Client, Error> {
    let mut connect_options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(10))
        .with_timeout(Duration::from_secs(30));

    let use_tls = config.ca_cert_path.is_some() || config.client_cert_path.is_some();

    if use_tls {
        let mut tls_options = TlsOptions::new();

        // Load CA cert if provided
        if let Some(ca_path) = &config.ca_cert_path {
            let ca_bytes = std::fs::read(ca_path).map_err(Error::IoError)?;
            let ca_cert = Certificate::from_pem(ca_bytes);
            tls_options = tls_options.ca_certificate(ca_cert);
        }

        // Load client cert and key if both provided
        if let (Some(cert_path), Some(key_path)) = (&config.client_cert_path, &config.client_key_path) {
            let cert_bytes = std::fs::read(cert_path).map_err(Error::IoError)?;
            let key_bytes  = std::fs::read(key_path).map_err(Error::IoError)?;
            let identity = Identity::from_pem(cert_bytes, key_bytes);
            tls_options = tls_options.identity(identity);
        }

        // Load domain name if provided
        if let Some(domain) = &config.servername {
            tls_options = tls_options.domain_name(domain);
        }

        connect_options = connect_options.with_tls(tls_options);
    }

    // Load Username and Password
    if let (Some(user), Some(pass)) = (&config.username, &config.password) {
        connect_options = connect_options.with_user(user, pass);
    }

    let endpoints: Vec<&str> = config.endpoints.iter().map(|s| s.as_str()).collect();
    Client::connect(endpoints, Some(connect_options)).await
}


type EtcdFdwResult<T> = std::result::Result<T, EtcdFdwError>;

impl ForeignDataWrapper<EtcdFdwError> for EtcdFdw {
    fn new(server: ForeignServer) -> EtcdFdwResult<EtcdFdw> {
        // Open connection to etcd specified through the server parameter
        let rt = tokio::runtime::Runtime::new().expect("Tokio runtime should be initialized");

        // Add parsing for the multi host connection string things here
        let connstr = match server.options.get("connstr") {
            Some(x) => x.clone(),
            None => return Err(EtcdFdwError::NoConnStr(())),
        };

        // TODO: username & pass should be captured seperately i.e. from CREATE USER MAPPING
        let cacert_path = server.options.get("ssl_ca").cloned();
        let cert_path = server.options.get("ssl_cert").cloned();
        let key_path  = server.options.get("ssl_key").cloned();
        let servername  = server.options.get("ssl_servername").cloned();
        let username = server.options.get("username").cloned();
        let password  = server.options.get("password").cloned();

        // ssl_cert + ssl_key must be both present or both absent
        // username + password must be both present or both absent
        require_pair(&cert_path, &key_path, EtcdFdwError::CertKeyMismatch(()))?;
        require_pair(&username, &password, EtcdFdwError::UserPassMismatch(()))?;

        let config = EtcdConfig {
            endpoints: vec![connstr],
            ca_cert_path: cacert_path,
            client_cert_path: cert_path,
            client_key_path: key_path,
            username: username,
            password: password,
            servername: servername,
        };

        let client = match rt.block_on(connect_etcd(config)) {
            Ok(x) => x,
            Err(e) => return Err(EtcdFdwError::ClientConnectionError(e.to_string())),
        };
        let prefix = match server.options.get("prefix") {
            Some(x) => x.clone(),
            None => String::from(""),
        };
        let fetch_results = vec![];

        Ok(Self {
            client,
            rt,
            prefix,
            fetch_results,
            fetch_key: false,
            fetch_value: false,
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        limit: &Option<Limit>,
        _options: &std::collections::HashMap<String, String>,
    ) -> Result<(), EtcdFdwError> {
        // Select get all rows as a result into a field of the struct
        // Build Query options from parameters
        let mut get_options = GetOptions::new().with_all_keys();
        match limit {
            Some(x) => get_options = get_options.with_limit(x.count),
            None => (),
        }
        // Check if columns contains key and value
        let colnames: Vec<String> = columns.iter().map(|x| x.name.clone()).collect();
        self.fetch_key = colnames.contains(&String::from("key"));
        self.fetch_value = colnames.contains(&String::from("value"));

        let result = self
            .rt
            .block_on(self.client.get(self.prefix.clone(), Some(get_options)));
        let mut result_unwrapped = match result {
            Ok(x) => x,
            Err(e) => return Err(EtcdFdwError::FetchError(e.to_string())),
        };
        let result_vec = result_unwrapped.take_kvs();
        self.fetch_results = result_vec;
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> EtcdFdwResult<Option<()>> {
        // Go through results row by row and drain the result vector
        if self.fetch_results.is_empty() {
            Ok(None)
        } else {
            Ok(self.fetch_results.drain(0..1).last().map(|x| {
                // Unpack x into a row
                let key = x.key_str().expect("Expected a key, but the key was empty");
                let value = x
                    .value_str()
                    .expect("Expected a value, but the value was empty");
                if self.fetch_key {
                    row.push("key", Some(Cell::String(key.to_string())));
                }
                if self.fetch_value {
                    row.push("value", Some(Cell::String(value.to_string())));
                }
            }))
        }
    }

    fn end_scan(&mut self) -> EtcdFdwResult<()> {
        self.fetch_results = vec![];
        self.fetch_key = false;
        self.fetch_value = false;
        Ok(())
    }

    fn begin_modify(
        &mut self,
        _options: &std::collections::HashMap<String, String>,
    ) -> Result<(), EtcdFdwError> {
        // This currently does nothing
        Ok(())
    }

    fn insert(&mut self, row: &Row) -> Result<(), EtcdFdwError> {
        let key_string = match row
            .cols
            .iter()
            .zip(row.cells.clone())
            .filter(|(name, _cell)| *name == "key")
            .last()
        {
            Some(x) => x.1.expect("The key column should be present").to_string(),
            None => return Err(EtcdFdwError::MissingColumn("key".to_string())),
        };
        let value_string = match row
            .cols
            .iter()
            .zip(row.cells.clone())
            .filter(|(name, _cell)| *name == "value")
            .last()
        {
            Some(x) => x.1.expect("The value column should be present").to_string(),
            None => return Err(EtcdFdwError::MissingColumn("value".to_string())),
        };
        let key = key_string.trim_matches(|x| x == '\'');
        let value = value_string.trim_matches(|x| x == '\'');

        // See if key already exists. Error if it does
        match self.rt.block_on(self.client.get(key, None)) {
            Ok(x) => {
                if let Some(y) = x.kvs().first() {
                    if y.key_str().expect("There should be a key string") == key {
                        return Err(EtcdFdwError::KeyAlreadyExists(format!("{}", key)));
                    }
                }
            }
            Err(e) => return Err(EtcdFdwError::FetchError(e.to_string())),
        }

        match self
            .rt
            .block_on(self.client.put(key, value, Some(PutOptions::new())))
        {
            Ok(_) => Ok(()),
            Err(e) => return Err(EtcdFdwError::UpdateError(e.to_string())),
        }
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> Result<(), EtcdFdwError> {
        let key_string = rowid.to_string();
        let key = key_string.trim_matches(|x| x == '\'');

        match self.rt.block_on(self.client.get(key, None)) {
            Ok(x) => {
                if let Some(y) = x.kvs().first() {
                    if y.key_str().expect("There should be a key string") != key {
                        return Err(EtcdFdwError::KeyDoesntExist(format!("{}", key)));
                    }
                }
            }
            Err(e) => return Err(EtcdFdwError::FetchError(e.to_string())),
        }

        let value_string = match new_row
            .cols
            .iter()
            .zip(new_row.cells.clone())
            .filter(|(name, _cell)| *name == "value")
            .last()
        {
            Some(x) => x.1.expect("The value column should be present").to_string(),
            None => return Err(EtcdFdwError::MissingColumn("value".to_string())),
        };
        let value = value_string.trim_matches(|x| x == '\'');

        match self.rt.block_on(self.client.put(key, value, None)) {
            Ok(_) => Ok(()),
            Err(e) => return Err(EtcdFdwError::UpdateError(e.to_string())),
        }
    }

    fn delete(&mut self, rowid: &Cell) -> Result<(), EtcdFdwError> {
        let key_string = rowid.to_string();
        let key = key_string.trim_matches(|x| x == '\'');

        let delete_options = DeleteOptions::new();

        match self.rt.block_on(self.client.get(key, None)) {
            Ok(x) => {
                if let Some(y) = x.kvs().first() {
                    if y.key_str().expect("There should be a key string") != key {
                        return Err(EtcdFdwError::KeyDoesntExist(format!("{}", key)));
                    }
                }
            }
            Err(e) => return Err(EtcdFdwError::FetchError(e.to_string())),
        }

        match self
            .rt
            .block_on(self.client.delete(key, Some(delete_options)))
        {
            Ok(x) => {
                if x.deleted() == 0 {
                    return Err(EtcdFdwError::UpdateError(format!(
                        "Deletion seemingly successful, but deleted count is {}",
                        x.deleted()
                    )));
                }
                Ok(())
            }
            Err(e) => Err(EtcdFdwError::UpdateError(e.to_string())),
        }
    }

    // fn get_rel_size(
    //     &mut self,
    //     _quals: &[Qual],
    //     _columns: &[Column],
    //     _sorts: &[Sort],
    //     _limit: &Option<Limit>,
    //     _options: &std::collections::HashMap<String, String>,
    // ) -> Result<(i64, i32), EtcdFdwError> {
    //     todo!("Get rel size is not yet implemented")
    // }

    fn end_modify(&mut self) -> Result<(), EtcdFdwError> {
        // This currently also does nothing
        Ok(())
    }
}
