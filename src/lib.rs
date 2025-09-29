use etcd_client::{Client, ConnectOptions, TlsOptions, Identity, Certificate, Error, DeleteOptions, GetOptions, KeyValue, PutOptions, SortTarget, SortOrder};
use std::time::Duration;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use pgrx::*;
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
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            username: None,
            password: None,
            servername: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
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

    #[error("Options 'prefix' and 'range_end' cannot be used together")]
    ConflictingPrefixAndRange,

    #[error("Options 'prefix' and 'key' should not be used together")]
    ConflictingPrefixAndKey,

    #[error("Key {0} doesn't exist in etcd")]
    KeyDoesntExist(String),

    #[error("Invalid option '{0}' with value '{1}'")]
    InvalidOption(String, String),

    #[error("Invalid sort field value '{0}'")]
    InvalidSortField(String),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),
}

impl From<EtcdFdwError> for ErrorReport {
    fn from(value: EtcdFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{}", value), "")
    }
}

/// Check whether dependent options exits
/// i.e username & pass, cert & key
fn require_pair(
    a: bool,
    b: bool,
    err: EtcdFdwError,
) -> Result<(), EtcdFdwError> {
    match (a, b) {
        (true, false) | (false, true) => Err(err),
        _ => Ok(()),
    }
}

/// Helper function for parsing timeouts
fn parse_timeout(
    options: &std::collections::HashMap<String, String>,
    key: &str,
    default: Duration,
) -> Result<Duration, EtcdFdwError> {
    if let Some(val) = options.get(key) {
        match val.parse::<u64>() {
            Ok(secs) => Ok(Duration::from_secs(secs)),
            Err(_) => Err(EtcdFdwError::InvalidOption(key.to_string(), val.clone())),
        }
    } else {
        Ok(default)
    }
}



/// Use this to connect to etcd.
/// Parse the certs/key paths and read them as bytes
/// Sets the `TlsOptions` if available to support sll connection
pub async fn connect_etcd(config: EtcdConfig) -> Result<Client, Error> {
    let mut connect_options = ConnectOptions::new()
        .with_connect_timeout(config.connect_timeout)
        .with_timeout(config.request_timeout);

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
        let mut config = EtcdConfig::default();

        // Open connection to etcd specified through the server parameter
        let rt = tokio::runtime::Runtime::new().expect("Tokio runtime should be initialized");

        // Add parsing for the multi host connection string things here
        let connstr = match server.options.get("connstr") {
            Some(x) => x.clone(),
            None => return Err(EtcdFdwError::NoConnStr(())),
        };

        // TODO: username & pass should be captured separately i.e. from CREATE USER MAPPING
        let cacert_path = server.options.get("ssl_ca").cloned();
        let cert_path = server.options.get("ssl_cert").cloned();
        let key_path  = server.options.get("ssl_key").cloned();
        let servername  = server.options.get("ssl_servername").cloned();
        let username = server.options.get("username").cloned();
        let password  = server.options.get("password").cloned();

        // Parse timeouts with defaults
        let connect_timeout = parse_timeout(&server.options, "connect_timeout", config.connect_timeout)?;
        let request_timeout = parse_timeout(&server.options, "request_timeout", config.request_timeout)?;

        // ssl_cert + ssl_key must be both present or both absent
        // username + password must be both present or both absent
        require_pair(cert_path.is_some(), key_path.is_some(), EtcdFdwError::CertKeyMismatch(()))?;
        require_pair(username.is_some(), password.is_some(), EtcdFdwError::UserPassMismatch(()))?;

        config = EtcdConfig {
            endpoints: vec![connstr],
            ca_cert_path: cacert_path,
            client_cert_path: cert_path,
            client_key_path: key_path,
            username: username,
            password: password,
            servername: servername,
            connect_timeout: connect_timeout,
            request_timeout: request_timeout,
        };

        let client = match rt.block_on(connect_etcd(config)) {
            Ok(x) => x,
            Err(e) => return Err(EtcdFdwError::ClientConnectionError(e.to_string())),
        };

        let fetch_results = vec![];

        Ok(Self {
            client,
            rt,
            fetch_results,
            fetch_key: false,
            fetch_value: false,
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        sort: &[Sort],
        limit: &Option<Limit>,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<(), EtcdFdwError> {
        // parse the options defined when `CREATE FOREIGN TABLE`
        let prefix = options.get("prefix").cloned();
        let range_end = options.get("range_end").cloned();
        let key_start = options.get("key").cloned();
        let keys_only = options.get("keys_only").map(|v| v == "true").unwrap_or(false);
        let revision = options.get("revision").and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
        let serializable = options.get("consistency").map(|v| v == "s").unwrap_or(false);
        let mut get_options = GetOptions::new();

        // prefix and range are mutually exclusive
        match (prefix.as_ref(), range_end.as_ref()) {
            (Some(_), Some(_)) => {
                return Err(EtcdFdwError::ConflictingPrefixAndRange);
            }
            (Some(_), None) => {
                get_options = get_options.with_prefix();
            }
            (None, Some(r)) => {
                get_options = get_options.with_range(r.clone());
            }
            (None, None) => {
                if key_start.is_none() {
                    get_options = get_options.with_all_keys();
                }
            }
        }

        if let Some(x) = limit {
            get_options = get_options.with_limit(x.count);
        }

        if keys_only {
            get_options = get_options.with_keys_only();
        }

        if revision > 0 {
            get_options = get_options.with_revision(revision);
        }

        if serializable {
            get_options = get_options.with_serializable();
        }

        // XXX Support for WHERE clause push-downs is pending
        // etcd doesn't have anything like WHERE clause because it 
        // a NOSQL database.
        // But may be we can still support some simple WHERE
        // conditions like '<', '>=', 'LIKE', '=' by mapping them
        // to key, range_end and prefix options.

        // sort pushdown
        if let Some(first_sort) = sort.first() {
            let field_name = first_sort.field.to_ascii_uppercase();

            if let Some(target) = SortTarget::from_str_name(&field_name) {
                let order = if first_sort.reversed {
                    SortOrder::Descend
                } else {
                    SortOrder::Ascend
                };

                get_options = get_options.with_sort(target, order);
            } else {
                return Err(EtcdFdwError::InvalidSortField(first_sort.field.clone()));
            }
        }

        // preference order : prefix > key_start > default "\0"
        // samllest possible valid key '\0'
        let key = prefix.clone()
                        .or_else(|| key_start.clone())
                        .unwrap_or_else(|| String::from("\0"));

        // Check if columns contains key and value
        let colnames: Vec<String> = columns.iter().map(|x| x.name.clone()).collect();
        self.fetch_key = colnames.contains(&String::from("key"));
        self.fetch_value = colnames.contains(&String::from("value"));

        let result = self
            .rt
            .block_on(self.client.get(key, Some(get_options)));
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

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> EtcdFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_SERVER_RELATION_ID {
                check_options_contain(&options, "connstr")?;

                let cacert_path_exists = check_options_contain(&options, "ssl_ca").is_ok();
                let cert_path_exists = check_options_contain(&options, "ssl_cert").is_ok();
                let username_exists = check_options_contain(&options, "username").is_ok();
                let password_exists = check_options_contain(&options, "password").is_ok();

                require_pair(cacert_path_exists, cert_path_exists, EtcdFdwError::CertKeyMismatch(()))?;
                require_pair(username_exists, password_exists, EtcdFdwError::UserPassMismatch(()))?;
            } else if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "rowid_column")?;

                let prefix_exists = check_options_contain(&options, "prefix").is_ok();
                let rannge_exists = check_options_contain(&options, "range_end").is_ok();
                let key_exists = check_options_contain(&options, "key").is_ok();

                if prefix_exists && rannge_exists {
                    return Err(EtcdFdwError::ConflictingPrefixAndRange);
                }

                if prefix_exists && key_exists {
                    return Err(EtcdFdwError::ConflictingPrefixAndKey);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod pg_test {

    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use std::time::Duration;

    use super::*;
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::SyncRunner,
        Container, GenericImage, ImageExt,
    };

    const CMD: [&'static str; 5] = [
        "/usr/local/bin/etcd",
        "--listen-client-urls",
        "http://0.0.0.0:2379",
        "--advertise-client-urls",
        "http://0.0.0.0:2379",
    ];

    fn create_container() -> (Container<GenericImage>, String) {
        let container = GenericImage::new("quay.io/coreos/etcd", "v3.6.4")
            .with_exposed_port(2379.tcp())
            .with_wait_for(WaitFor::message_on_either_std(
                "ready to serve client requests",
            ))
            .with_privileged(true)
            .with_cmd(CMD)
            .with_startup_timeout(Duration::from_secs(90))
            .start()
            .expect("An etcd image was supposed to be started");

        let host = container
            .get_host()
            .expect("Host-address should be available");

        let port = container
            .get_host_port_ipv4(2379.tcp())
            .expect("Exposed host port should be available");

        let url = format!("{}:{}", host, port);
        (container, url)
    }

    fn create_fdt(url: String) -> () {
        Spi::run("CREATE FOREIGN DATA WRAPPER etcd_fdw handler etcd_fdw_handler validator etcd_fdw_validator;").expect("FDW should have been created");

        // Create a server
        Spi::run(
            format!(
                "CREATE SERVER etcd_test_server FOREIGN DATA WRAPPER etcd_fdw options(connstr '{}')",
                url
            )
            .as_str(),
        )
        .expect("Server should have been created");

        // Create a foreign table
        Spi::run("CREATE FOREIGN TABLE test (key text, value text) server etcd_test_server options (rowid_column 'key')").expect("Test table should have been created");
    }

    #[pg_test]
    fn test_create_table() {
        let (_container, url) = create_container();

        create_fdt(url);
    }
    #[pg_test]
    fn test_insert_select() {
        let (_container, url) = create_container();

        create_fdt(url);

        // Insert into the foreign table
        Spi::run("INSERT INTO test (key, value) VALUES ('foo','bar'),('bar','baz')")
            .expect("INSERT should work");

        let query_result = Spi::get_two::<String, String>("SELECT * FROM test WHERE key='foo'")
            .expect("Select should work");

        assert_eq!((Some(format!("foo")), Some(format!("bar"))), query_result);
        let query_result = Spi::get_two::<String, String>("SELECT * FROM test WHERE key='bar'")
            .expect("SELECT should work");

        assert_eq!((Some(format!("bar")), Some(format!("baz"))), query_result);
    }

    #[pg_test]
    fn test_update() {
        let (_container, url) = create_container();

        create_fdt(url);

        Spi::run("INSERT INTO test (key, value) VALUES ('foo','bar'),('bar','baz')")
            .expect("INSERT should work");

        Spi::run("UPDATE test SET value='test_successful'").expect("UPDATE should work");

        let query_result =
            Spi::get_one::<String>("SELECT value FROM test;").expect("SELECT should work");

        assert_eq!(Some(format!("test_successful")), query_result);
    }

    #[pg_test]
    fn test_delete() {
        let (_container, url) = create_container();

        create_fdt(url);

        Spi::run("INSERT INTO test (key, value) VALUES ('foo','bar'),('bar','baz')")
            .expect("INSERT should work");

        Spi::run("DELETE FROM test").expect("DELETE should work");

        let query_result = Spi::get_one::<String>("SELECT value FROM test;");

        assert_eq!(Err(spi::SpiError::InvalidPosition), query_result);
    }
}
