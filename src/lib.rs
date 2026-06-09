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
    // `client` is declared before `rt` so that it is dropped first: the etcd
    // client's background work can wind down while its runtime is still alive.
    // The connection is established lazily (see `ensure_connected`).
    client: Option<Client>,
    rt: Runtime,
    config: EtcdConfig,
    fetch_results: Vec<KeyValue>,
    fetch_pos: usize,
    tgt_cols: Vec<Column>,
}

#[derive(Clone)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub user: Option<String>,
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
            user: None,
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

    #[error("User and Password must both be specified through user mappings.")]
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

    #[error("Failed to initialize the async runtime: {0}")]
    RuntimeInitError(String),

    #[error("Column '{0}' must not be NULL")]
    NullValue(String),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),
}

impl From<EtcdFdwError> for ErrorReport {
    fn from(value: EtcdFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{}", value), "")
    }
}

/// Check whether dependent options exits
/// i.e user & pass, cert & key
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

    // Load User and Password
    if let (Some(user), Some(pass)) = (&config.user, &config.password) {
        connect_options = connect_options.with_user(user, pass);
    }

    let endpoints: Vec<&str> = config.endpoints.iter().map(|s| s.as_str()).collect();
    Client::connect(endpoints, Some(connect_options)).await
}


type EtcdFdwResult<T> = std::result::Result<T, EtcdFdwError>;

/// Read a text column's value out of a `Cell` directly, instead of going
/// through `Display` (which renders a string as `'foo'`). This returns the
/// exact value the user supplied, so keys/values that contain single quotes are
/// preserved rather than mangled by quote-stripping.
fn cell_to_text(cell: &Cell) -> String {
    match cell {
        Cell::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Extract a required text column's value from a row, returning an error if the
/// column is absent or its value is NULL (instead of panicking).
fn required_cell(row: &Row, col: &str) -> EtcdFdwResult<String> {
    match row
        .cols
        .iter()
        .zip(row.cells.iter())
        .filter(|(name, _)| name.as_str() == col)
        .last()
    {
        Some((_, Some(cell))) => Ok(cell_to_text(cell)),
        Some((_, None)) => Err(EtcdFdwError::NullValue(col.to_string())),
        None => Err(EtcdFdwError::MissingColumn(col.to_string())),
    }
}

impl EtcdFdw {
    /// Establish the etcd connection on first use. Connecting lazily keeps
    /// `new()` cheap (it can run at planning time) and avoids opening a
    /// connection for statements that are planned but never executed.
    fn ensure_connected(&mut self) -> EtcdFdwResult<()> {
        if self.client.is_none() {
            let client = self
                .rt
                .block_on(connect_etcd(self.config.clone()))
                .map_err(|e| EtcdFdwError::ClientConnectionError(e.to_string()))?;
            self.client = Some(client);
        }
        Ok(())
    }
}

impl ForeignDataWrapper<EtcdFdwError> for EtcdFdw {
    fn new(server: ForeignServer) -> EtcdFdwResult<EtcdFdw> {
        // Connection string for the etcd server (required).
        let connstr = match server.options.get("connstr") {
            Some(x) => x.clone(),
            None => return Err(EtcdFdwError::NoConnStr(())),
        };

        let cacert_path = server.options.get("ssl_ca").cloned();
        let cert_path = server.options.get("ssl_cert").cloned();
        let key_path = server.options.get("ssl_key").cloned();
        let servername = server.options.get("ssl_servername").cloned();

        // Parse timeouts with defaults
        let connect_timeout =
            parse_timeout(&server.options, "connect_timeout", Duration::from_secs(10))?;
        let request_timeout =
            parse_timeout(&server.options, "request_timeout", Duration::from_secs(30))?;

        // ssl_cert + ssl_key must be both present or both absent
        require_pair(
            cert_path.is_some(),
            key_path.is_some(),
            EtcdFdwError::CertKeyMismatch(()),
        )?;

        // Read user/password from the user mapping of the current role.
        //
        // This deliberately runs BEFORE the tokio runtime is created. The raw
        // `pg_sys::GetUserMapping` call raises a Postgres ERROR (a C longjmp)
        // when the current role has no user mapping for this server. That
        // longjmp unwinds straight to the FDW callback boundary and skips the
        // Rust destructors in between. As long as no `tokio::runtime::Runtime`
        // is alive at that point, nothing important is skipped and Postgres
        // reports a clean "user mapping not found" error -- instead of leaking
        // the runtime's worker threads, which previously destabilized the
        // backend and led to a segfault.
        let mut user = None;
        let mut password = None;

        unsafe {
            let usermapping = pg_sys::GetUserMapping(pg_sys::GetUserId(), server.server_oid);
            if !usermapping.is_null() {
                let options = (*usermapping).options;
                pgrx::memcx::current_context(|mcx| {
                    if let Some(list) =
                        pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(
                            options, mcx,
                        )
                    {
                        for option in list.iter() {
                            let option = *option as *mut pg_sys::DefElem;
                            if option.is_null() {
                                continue;
                            }
                            let name = std::ffi::CStr::from_ptr((*option).defname).to_str();
                            let value =
                                std::ffi::CStr::from_ptr(pg_sys::defGetString(option)).to_str();
                            if let (Ok(name), Ok(value)) = (name, value) {
                                match name {
                                    "user" => user = Some(value.to_string()),
                                    "password" => password = Some(value.to_string()),
                                    _ => {}
                                }
                            }
                        }
                    }
                });
            }
        }

        let config = EtcdConfig {
            endpoints: vec![connstr],
            ca_cert_path: cacert_path,
            client_cert_path: cert_path,
            client_key_path: key_path,
            user,
            password,
            servername,
            connect_timeout,
            request_timeout,
        };

        // Use the framework's current-thread runtime helper. A Postgres backend
        // is single-threaded and the FDW only ever `block_on`s, so there is no
        // reason to spawn worker threads (one per CPU core, as the multi-threaded
        // `Runtime::new()` does). Avoiding a worker pool means no background
        // threads that could receive Postgres signals off the main thread, and
        // none that could be leaked if a Postgres error unwinds through this
        // struct. `create_async_runtime()` is exactly
        // `Builder::new_current_thread().enable_all().build()`.
        let rt = create_async_runtime().map_err(|e| EtcdFdwError::RuntimeInitError(e.to_string()))?;

        // The etcd connection is established lazily in `begin_scan` /
        // `begin_modify`: `new()` must stay cheap because the framework can call
        // it at planning time, possibly for a statement that is never executed.
        Ok(Self {
            client: None,
            rt,
            config,
            fetch_results: vec![],
            fetch_pos: 0,
            tgt_cols: Vec::new(),
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
        let mut qual_key_start: Option<String> = None;
        let mut qual_prefix: Option<String> = None;
        let mut qual_range_end: Option<String> = None;
        let mut get_options = GetOptions::new();

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

        // WHERE clause pushdown
        for q in _quals {
            // only pushdown "key"
            if q.field != "key" {
                continue;
            }

            // extract string value
            let v = match &q.value {
                Value::Cell(Cell::String(s)) => s.clone(),
                _ => continue,
            };

            match q.operator.as_str() {
                "=" => {
                    // equal: start at v, end at v+"\0"
                    qual_key_start = Some(v.clone());
                    qual_range_end = Some(format!("{}\0", v));
                }
                ">=" => {
                    // greater or equal: start at v
                    qual_key_start = Some(v.clone());
                }
                ">" => {
                    // greater than: start at v+"\0"
                    qual_key_start = Some(format!("{}\0", v));
                }
                "<" => {
                    // less than: end at v
                    qual_range_end = Some(v.clone());
                }
                "<=" => {
                    // less or equal: end at v+"\0"
                    qual_range_end = Some(format!("{}\0", v));
                }
                "~~" => {
                    // LIKE operator with % suffix only
                    if let Some(pref) = v.strip_suffix('%') {
                        qual_prefix = Some(pref.to_string());
                    }
                }
                _ => {}
            }
        }

        // Determine the effective prefix based on FDW and WHERE clause options
        // If both are present, ensure one is a prefix of the other
        // Otherwise, no data will be fetched
        // If only one is present, use that as the prefix
        let eff_prefix = match (prefix.as_ref(), qual_prefix.as_ref()) {
            (Some(_fdw), Some(_where)) => {
                if _where.starts_with(_fdw) {
                    Some(_where.clone())
                } else if _fdw.starts_with(_where) {
                    Some(_fdw.clone())
                } else {
                    return Ok(());
                }
            }
            (Some(_fdw), None) => Some(_fdw.clone()),
            (None, Some(_where)) => Some(_where.clone()),
            (None, None) => None,
        };

        // Determine the effective key start based on FDW and WHERE clause options
        // If both are present, take the larger one
        // Otherwise, take whichever is present
        // If neither is present, start from the beginning
        let eff_key_start = match (&qual_key_start, &key_start) {
            (Some(_where), Some(_fdw)) => {
                if _where > _fdw {
                    _where.clone()
                } else {
                    _fdw.clone()
                }
            }
            (Some(_where), None) => _where.clone(),
            (None, Some(_fdw)) => _fdw.clone(),
            (None, None) => "\0".to_string(), // start from the beginning
        };

        // Determine the effective range end based on FDW and WHERE clause options
        // If both are present, take the smaller one
        // Otherwise, take whichever is present
        // If neither is present, go to the end
        let mut eff_range_end = match (&qual_range_end, &range_end) {
            (Some(_where), Some(_fdw)) => {
                if _where < _fdw {
                    _where.clone()
                } else {
                    _fdw.clone()
                }
            }
            (Some(_where), None) => _where.clone(),
            (None, Some(_fdw)) => _fdw.clone(),
            (None, None) => "\u{10FFFF}".to_string(), // go to the end
        };

        // Compute range_end for prefix
        // If a prefix is provided, calculate the range_end by incrementing the last byte of the prefix
        // This ensures that the range_end is exclusive and covers all keys starting with the prefix
        if let Some(p) = &eff_prefix {
            let mut bytes = p.as_bytes().to_vec();
            for i in (0..bytes.len()).rev() {
                if bytes[i] < 0xFF {
                    bytes[i] += 1;
                    bytes.truncate(i + 1);
                    // Incrementing a byte inside a multi-byte UTF-8 sequence can
                    // produce invalid UTF-8. If that happens, skip narrowing the
                    // range here rather than panicking; the broader range is
                    // still correct because Postgres re-checks the qualifier on
                    // the rows the FDW returns.
                    if let Ok(prefix_range_end) = String::from_utf8(bytes) {
                        // Ensure the calculated range_end does not exceed the effective range_end
                        if prefix_range_end < eff_range_end {
                            eff_range_end = prefix_range_end;
                        }
                    }
                    break;
                }
            }
        }

        // Determine the effective key to start the scan
        // If a prefix is provided, use it as the base key and enable prefix-based scanning
        // Otherwise, use the effective key start
        let key = match &eff_prefix {
            Some(p) => {
                get_options = get_options.with_prefix();
                // Ensure the key starts from the larger of the prefix or the effective key start
                std::cmp::max(eff_key_start.clone(), p.clone())
            }
            None => eff_key_start.clone(),
        };

        get_options = get_options.with_range(eff_range_end);

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

        self.ensure_connected()?;
        let client = self
            .client
            .as_mut()
            .expect("client must be connected after ensure_connected");
        let result = self.rt.block_on(client.get(key, Some(get_options)));
        let mut result_unwrapped = match result {
            Ok(x) => x,
            Err(e) => return Err(EtcdFdwError::FetchError(e.to_string())),
        };
        self.fetch_results = result_unwrapped.take_kvs();
        self.fetch_pos = 0;
        self.tgt_cols = columns.to_vec();
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> EtcdFdwResult<Option<()>> {
        // Walk the buffered results with a cursor. Draining the front of the
        // vector on every call would be O(n) per row, i.e. O(n^2) per scan.
        if self.fetch_pos >= self.fetch_results.len() {
            return Ok(None);
        }

        let kv = &self.fetch_results[self.fetch_pos];
        // etcd keys/values are arbitrary bytes; replace any invalid UTF-8
        // instead of panicking, since the columns are exposed as `text`.
        let key = String::from_utf8_lossy(kv.key()).into_owned();
        let value = String::from_utf8_lossy(kv.value()).into_owned();
        self.fetch_pos += 1;

        for tgt_col in &self.tgt_cols {
            if tgt_col.name == "key" {
                row.push(&tgt_col.name, Some(Cell::String(key.clone())));
            }
            if tgt_col.name == "value" {
                row.push(&tgt_col.name, Some(Cell::String(value.clone())));
            }
        }

        Ok(Some(()))
    }

    fn end_scan(&mut self) -> EtcdFdwResult<()> {
        self.fetch_results = vec![];
        self.fetch_pos = 0;
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
        let key_string = required_cell(row, "key")?;
        let value_string = required_cell(row, "value")?;
        let key = key_string.as_str();
        let value = value_string.as_str();

        self.ensure_connected()?;
        let client = self
            .client
            .as_mut()
            .expect("client must be connected after ensure_connected");

        // Reject duplicates: error if the key already exists.
        match self.rt.block_on(client.get(key, None)) {
            Ok(x) => {
                if x.kvs().iter().any(|kv| kv.key() == key.as_bytes()) {
                    return Err(EtcdFdwError::KeyAlreadyExists(key.to_string()));
                }
            }
            Err(e) => return Err(EtcdFdwError::FetchError(e.to_string())),
        }

        match self
            .rt
            .block_on(client.put(key, value, Some(PutOptions::new())))
        {
            Ok(_) => Ok(()),
            Err(e) => Err(EtcdFdwError::UpdateError(e.to_string())),
        }
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> Result<(), EtcdFdwError> {
        let key_string = cell_to_text(rowid);
        let key = key_string.as_str();

        let value_string = required_cell(new_row, "value")?;
        let value = value_string.as_str();

        self.ensure_connected()?;
        let client = self
            .client
            .as_mut()
            .expect("client must be connected after ensure_connected");

        // The key must already exist; an exact `get` returns it or nothing.
        match self.rt.block_on(client.get(key, None)) {
            Ok(x) => {
                if !x.kvs().iter().any(|kv| kv.key() == key.as_bytes()) {
                    return Err(EtcdFdwError::KeyDoesntExist(key.to_string()));
                }
            }
            Err(e) => return Err(EtcdFdwError::FetchError(e.to_string())),
        }

        match self.rt.block_on(client.put(key, value, None)) {
            Ok(_) => Ok(()),
            Err(e) => Err(EtcdFdwError::UpdateError(e.to_string())),
        }
    }

    fn delete(&mut self, rowid: &Cell) -> Result<(), EtcdFdwError> {
        let key_string = cell_to_text(rowid);
        let key = key_string.as_str();

        self.ensure_connected()?;
        let client = self
            .client
            .as_mut()
            .expect("client must be connected after ensure_connected");

        match self.rt.block_on(client.get(key, None)) {
            Ok(x) => {
                if !x.kvs().iter().any(|kv| kv.key() == key.as_bytes()) {
                    return Err(EtcdFdwError::KeyDoesntExist(key.to_string()));
                }
            }
            Err(e) => return Err(EtcdFdwError::FetchError(e.to_string())),
        }

        match self
            .rt
            .block_on(client.delete(key, Some(DeleteOptions::new())))
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

                require_pair(cacert_path_exists, cert_path_exists, EtcdFdwError::CertKeyMismatch(()))?;
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
            } else if oid == pg_sys::BuiltinOid::UserMappingRelationId.value() {
                let user_exists = check_options_contain(&options, "user").is_ok();
                let password_exists = check_options_contain(&options, "password").is_ok();

                require_pair(user_exists, password_exists, EtcdFdwError::UserPassMismatch(()))?;
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
    use etcd_client::Permission;
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

    const ETCD_USER: &str = "root";
    const ETCD_PASS: &str = "secret";

    // Setup etcd root role/user and enable authentication
    async fn etcd_auth_setup(endpoint: String) {
        let mut client: Client = Client::connect([endpoint], None)
            .await
            .expect("connect etcd");

        // add root user and role
        client.role_add("root").await.expect("add role");
        client.user_add(ETCD_USER, ETCD_PASS, None)
            .await
            .expect("add user");

        client.user_grant_role(ETCD_USER, "root")
            .await
            .expect("grant role");

        client.auth_enable().await.expect("enable auth");
    }

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
        let rt = tokio::runtime::Runtime::new().expect("Tokio runtime should be initialized");
        rt.block_on(etcd_auth_setup(url.clone()));
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

        // Create a user mapping
        Spi::run(
            format!(
                "CREATE USER MAPPING FOR CURRENT_USER SERVER etcd_test_server options (user '{}', password '{}')",
                ETCD_USER, ETCD_PASS
            )
            .as_str(),
        )
        .expect("User mapping should have been created");

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

    #[pg_test]
    fn test_select_value_only_with_key_filter() {
        // Test for issue #26: selecting only value column with WHERE clause on key
        let (_container, url) = create_container();

        create_fdt(url);

        // Insert test data
        Spi::run("INSERT INTO test (key, value) VALUES ('key1','value1'),('key2','value2'),('key3','value3')")
            .expect("INSERT should work");

        // Test 1: SELECT value WHERE key = 'key2' should return the value
        let query_result = Spi::get_one::<String>("SELECT value FROM test WHERE key = 'key2'")
            .expect("SELECT value with key filter should work");

        assert_eq!(Some(format!("value2")), query_result);

        // Test 2: SELECT key WHERE key = 'key1' should also work
        let query_result = Spi::get_one::<String>("SELECT key FROM test WHERE key = 'key1'")
            .expect("SELECT key with key filter should work");

        assert_eq!(Some(format!("key1")), query_result);

        // Test 3: SELECT * WHERE key = 'key3' should work (baseline)
        let query_result = Spi::get_two::<String, String>("SELECT * FROM test WHERE key = 'key3'")
            .expect("SELECT * with key filter should work");

        assert_eq!((Some(format!("key3")), Some(format!("value3"))), query_result);
    }

    #[pg_test]
    fn test_update_value_only_with_key_filter() {
        // Test for issue #26: UPDATE with only value column when key is in WHERE clause
        let (_container, url) = create_container();

        create_fdt(url);

        // Insert test data
        Spi::run("INSERT INTO test (key, value) VALUES ('gather/78','original_value'),('gather/84','other_value')")
            .expect("INSERT should work");

        // Update without including key column in SET clause
        Spi::run("UPDATE test SET value = 'updated_value' WHERE key = 'gather/84'")
            .expect("UPDATE with key filter should work");

        // Verify the update worked
        let query_result = Spi::get_one::<String>("SELECT value FROM test WHERE key = 'gather/84'")
            .expect("SELECT after UPDATE should work");

        assert_eq!(Some(format!("updated_value")), query_result);

        // Verify other row was not affected
        let query_result = Spi::get_one::<String>("SELECT value FROM test WHERE key = 'gather/78'")
            .expect("SELECT other row should work");

        assert_eq!(Some(format!("original_value")), query_result);
    }

    #[pg_test]
    fn test_user_mapping_validation() {
        let (_container, url) = create_container();

        create_fdt(url.clone());

        // Insert test data
        Spi::run("INSERT INTO test (key, value) VALUES ('/gather', 'data')")
            .expect("INSERT should work");

        // Test 1: User mapping with invalid credentials (should fail)
        Spi::run("ALTER USER MAPPING FOR CURRENT_USER SERVER etcd_test_server OPTIONS (SET password 'wrong_password');")
            .expect("Alter user mapping should work");

        let result = std::panic::catch_unwind(|| {
            Spi::run("SELECT * FROM test;").expect("SELECT should work");
        });

        assert!(result.is_err(), "Expected SELECT to fail due to invalid user mapping");

        // Setup: create a role and user with limited permissions in etcd
        let rt = tokio::runtime::Runtime::new().expect("Tokio runtime should be initialized");
        rt.block_on(
            async {
                let mut client: Client = Client::connect([url.clone()], Some(ConnectOptions::new().with_user(ETCD_USER, ETCD_PASS)))
                    .await
                    .expect("connect etcd");
                client.role_add("rw_role").await.expect("add role");
                // role with read and write permissions on keys starting with "/"
                client.role_grant_permission("rw_role", Permission::with_from_key(Permission::read_write("/")))
                    .await
                    .expect("grant permission");
                client.user_add("etcd_user", "secret", None)
                    .await
                    .expect("add user");
                client.user_grant_role("etcd_user", "rw_role")
                    .await
                    .expect("grant role");
            }
        );

        // Alter user mapping to use the new limited permissions user
        Spi::run("ALTER USER MAPPING FOR CURRENT_USER SERVER etcd_test_server OPTIONS (SET user 'etcd_user', SET password 'secret');")
            .expect("Alter user mapping should work");

        // Test 2: Selecting a key outside of the user's permissions (should fail)
        let invalid_result =  std::panic::catch_unwind(|| {
            Spi::run("SELECT * FROM test;").expect("SELECT should work");
        });

        assert!(invalid_result.is_err(), "Expected SELECT to fail due to insufficient permissions");

        // Test 3: Selecting a key within the user's permissions
        let result = Spi::get_two::<String, String>("SELECT * FROM test WHERE key = '/gather'")
            .expect("SELECT with proper permissions should work");

        assert_eq!((Some(format!("/gather")), Some(format!("data"))), result);
    }

    // Regression test for the SIGSEGV (signal 11) that occurred when a scan ran
    // under a role with no user mapping. `GetUserMapping` raises a Postgres
    // ERROR (a C longjmp) in that case; previously a multi-threaded tokio
    // runtime was already alive in `new()`, so the longjmp skipped its `Drop`
    // and leaked worker threads until the backend crashed. The FDW now reads the
    // user mapping before creating a current-thread runtime, so this path must
    // surface a clean error and never crash the backend.
    #[pg_test]
    fn test_scan_without_user_mapping_errors_cleanly() {
        let (_container, url) = create_container();

        Spi::run("CREATE FOREIGN DATA WRAPPER etcd_fdw handler etcd_fdw_handler validator etcd_fdw_validator;")
            .expect("FDW should have been created");
        Spi::run(
            format!(
                "CREATE SERVER etcd_test_server FOREIGN DATA WRAPPER etcd_fdw options(connstr '{}')",
                url
            )
            .as_str(),
        )
        .expect("Server should have been created");
        Spi::run("CREATE FOREIGN TABLE test (key text, value text) server etcd_test_server options (rowid_column 'key')")
            .expect("Test table should have been created");

        // Intentionally NO `CREATE USER MAPPING`: the scan must fail with a clean
        // error rather than crashing the backend. If the old segfault regressed,
        // the backend would die here and abort the whole test process.
        let result = std::panic::catch_unwind(|| {
            Spi::run("SELECT * FROM test;").expect("SELECT should work");
        });

        assert!(
            result.is_err(),
            "expected a clean error for a scan with no user mapping, not a crash"
        );
    }

    // Regression test for the data-corruption bug where insert/update/delete ran
    // the cell through Display (which renders a String as `'foo'`) and then
    // stripped surrounding single quotes, mangling any key/value that legitimately
    // contains quotes. They must now round-trip exactly.
    #[pg_test]
    fn test_quotes_in_key_and_value_round_trip() {
        let (_container, url) = create_container();
        create_fdt(url);

        // key = o'clock, value = 'quoted'  (SQL doubles each embedded quote).
        Spi::run("INSERT INTO test (key, value) VALUES ('o''clock', '''quoted''')")
            .expect("INSERT should work");

        let row = Spi::get_two::<String, String>("SELECT key, value FROM test")
            .expect("SELECT should work");

        assert_eq!(
            (Some("o'clock".to_string()), Some("'quoted'".to_string())),
            row
        );
    }
}
