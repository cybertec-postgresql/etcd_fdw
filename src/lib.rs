use etcd::Client;
use pgrx::prelude::*;
use supabase_wrappers::prelude::*;

::pgrx::pg_module_magic!(name, version);

#[wrappers_fdw(
    version = "0.0.0",
    author = "Cybertec PostgreSQL International GmbH",
    error_type = "EtcdFdwError"
)]
pub struct EtcdFdw {
    client: Client,
}

pub enum EtcdFdwError {}

impl ForeignDataWrapper<EtcdFdw> for EtcdFdw {
    fn new(server: ForeignServer) -> Result<Self, EtcdFdwError> {
        // Open connection to etcd specified through the server parameter

        let client = Client::new([server.server_name], None);

        Self { client }
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<(), EtcdFdw> {
        todo!()
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, EtcdFdw> {
        todo!()
    }

    fn end_scan(&mut self) -> Result<(), EtcdFdw> {
        todo!()
    }

    fn begin_modify(
        &mut self,
        _options: &std::collections::HashMap<String, String>,
    ) -> Result<(), EtcdFdw> {
        todo!()
    }

    fn insert(&mut self, _row: &Row) -> Result<(), EtcdFdw> {
        todo!()
    }

    fn update(&mut self, _rowid: &Cell, _new_row: &Row) -> Result<(), EtcdFdw> {
        todo!()
    }

    fn delete(&mut self, _rowid: &Cell) -> Result<(), EtcdFdw> {
        todo!()
    }

    fn get_rel_size(
        &mut self,
        _quals: &[Qual],
        _columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &std::collections::HashMap<String, String>,
    ) -> Result<(i64, i32), EtcdFdw> {
        todo!()
    }

    fn end_modify(&mut self) -> Result<(), EtcdFdw> {
        todo!()
    }
}
