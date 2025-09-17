use etcd_client::{Client, GetOptions, KeyValue};
use futures::Stream;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::pg_sys::ErrorContextCallback;
use pgrx::PgSqlErrorCode;
use supabase_wrappers::prelude::*;
use thiserror::Error;
use tokio::runtime::*;

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
}

#[derive(Error, Debug)]
pub enum EtcdFdwError {
    #[error("Failed to fetch from etcd")]
    FetchError(String),
}

impl From<EtcdFdwError> for ErrorReport {
    fn from(_value: EtcdFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
    }
}

type EtcdFdwResult<T> = std::result::Result<T, EtcdFdwError>;

impl ForeignDataWrapper<EtcdFdwError> for EtcdFdw {
    fn new(server: ForeignServer) -> EtcdFdwResult<EtcdFdw> {
        // Open connection to etcd specified through the server parameter
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt
            .block_on(Client::connect(&[server.server_name], None))
            .unwrap();
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
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<(), EtcdFdwError> {
        // Select get all rows as a result into a field of the struct
        // Build Query options from parameters
        let mut get_options = GetOptions::new().with_all_keys();
        match limit {
            Some(x) => get_options = get_options.with_limit(x.count),
            None => (),
        }
        // Also do quals, columns and sorts.

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
                todo!()
            }))
        }
    }

    fn end_scan(&mut self) -> EtcdFdwResult<()> {
        todo!()
    }

    fn begin_modify(
        &mut self,
        _options: &std::collections::HashMap<String, String>,
    ) -> Result<(), EtcdFdwError> {
        todo!()
    }

    fn insert(&mut self, _row: &Row) -> Result<(), EtcdFdwError> {
        todo!()
    }

    fn update(&mut self, _rowid: &Cell, _new_row: &Row) -> Result<(), EtcdFdwError> {
        todo!()
    }

    fn delete(&mut self, _rowid: &Cell) -> Result<(), EtcdFdwError> {
        todo!()
    }

    fn get_rel_size(
        &mut self,
        _quals: &[Qual],
        _columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &std::collections::HashMap<String, String>,
    ) -> Result<(i64, i32), EtcdFdwError> {
        todo!()
    }

    fn end_modify(&mut self) -> Result<(), EtcdFdwError> {
        todo!()
    }
}
