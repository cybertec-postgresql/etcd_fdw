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
create extension etcd_fdw;```
```sql
CREATE foreign data wrapper etcd_fdw handler etcd_fdw_handler validator etcd_fdw_validator;```
```sql
CREATE SERVER my_etcd_server foreign data wrapper etcd_fdw options (connstr '127.0.0.1:2379');```
```sql
CREATE foreign table test (key text, value text) server my_etcd_server;```
```sql
SELECT * FROM test;```
Which would yield something like:
`
 key | value
-----+-------
 bar | baz
 foo | bar
(2 rows)
`
