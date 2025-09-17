use etcd_client::{Client, DeleteOptions, GetOptions, KeyValue, PutOptions};
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

type EtcdFdwResult<T> = std::result::Result<T, EtcdFdwError>;

impl ForeignDataWrapper<EtcdFdwError> for EtcdFdw {
    fn new(server: ForeignServer) -> EtcdFdwResult<EtcdFdw> {
        // Open connection to etcd specified through the server parameter
        let rt = tokio::runtime::Runtime::new().expect("Tokio runtime should be initialized");

        // Add parsing for the multi host connection string things here
        let server_name = match server.options.get("connstr") {
            Some(x) => x,
            None => return Err(EtcdFdwError::NoConnStr(())),
        };

        let client = match rt.block_on(Client::connect(&[server_name], None)) {
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
