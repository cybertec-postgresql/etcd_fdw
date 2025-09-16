use etcd_client::Client;
use futures::Stream;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use supabase_wrappers::prelude::*;
use tokio::runtime::*;

#[wrappers_fdw(
    version = "0.0.1",
    author = "Cybertec PostgreSQL International GmbH",
    error_type = "EtcdFdwError"
)]
pub(crate) struct EtcdFdw {
    client: Client,
    rt: Runtime,
}

pub enum EtcdFdwError {}

impl From<EtcdFdwError> for ErrorReport {
    fn from(_value: EtcdFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
    }
}

type EtcdFdwResult<T> = std::result::Result<T, EtcdFdwError>;

impl ForeignDataWrapper<EtcdFdwError> for EtcdFdw {
    fn new(server: ForeignServer) -> Result<EtcdFdw, EtcdFdwError> {
        // Open connection to etcd specified through the server parameter
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt
            .block_on(Client::connect(&[server.server_name], None))
            .unwrap();

        // Perform health checks and throw error on unhealthy

        Ok(Self { client, rt })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<(), EtcdFdwError> {
        todo!()
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, EtcdFdwError> {
        todo!()
    }

    fn end_scan(&mut self) -> Result<(), EtcdFdwError> {
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
