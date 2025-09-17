# etcd_fdw
A foreign data wrapper around etcd for postgres

## Setup
- Install pgrx on your machine `cargo install --locked cargo-pgrx --version 0.14.3`
- Setup pgrx `cargo pgrx init`
- Have some kind of etcd you want to test and run

## Build
- To build simply run `cargo pgrx run` with or without the `--release` flag

## Try out something yourself
Currently we can only read from etcd.
Here's an example of how to connect to your etcd instance and read some KVs
```sql
CREATE EXTENSION etcd_fdw;

CREATE FOREIGN DATA WRAPPER etcd_fdw HANDLER etcd_fdw_handler VALIDATOR etcd_fdw_validator;

CREATE SERVER my_etcd_server FOREIGN DATA WRAPPER etcd_fdw OPTIONS (connstr '127.0.0.1:2379');

CREATE FOREIGN TABLE test (key text, value text) SERVER my_etcd_server;

SELECT * FROM test;
```

Which would yield something like:
```terminal
 key | value
-----+-------
 bar | baz
 foo | bar
(2 rows)
```
