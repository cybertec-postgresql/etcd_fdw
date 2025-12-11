# etcd_fdw

A foreign data wrapper around etcd for postgres

## Setup

- Install pgrx on your machine `cargo install --locked cargo-pgrx --version 0.16.1`
- Setup pgrx `cargo pgrx init`
- Install protoc and protobuf (needed by etcd-client)
  - Instructions can be found [on the official site](https://protobuf.dev/installation/)
- Have some kind of etcd you want to test and run

## Build

- To build simply run `cargo pgrx run` with or without the `--release` flag

## Try out something yourself

Currently we can only read from etcd.
Here's an example of how to connect to your etcd instance and read some KVs

```sql
create extension etcd_fdw;
```

```sql
CREATE foreign data wrapper etcd_fdw handler etcd_fdw_handler validator etcd_fdw_validator;
```

```sql
CREATE SERVER my_etcd_server foreign data wrapper etcd_fdw options (connstr '127.0.0.1:2379');
```

```sql
CREATE foreign table test (key text, value text) server my_etcd_server options(rowid_column 'key');
```

```sql
SELECT * FROM test;
```

Which would yield something like:

```
 key | value
-----+-------
 bar | baz
 foo | bar
(2 rows)
```

the rowid option is required. As are the names key and value for the columns.

## Pushdowning

`etcd_fdw` supports push-down of filters, order by and limit clauses to the etcd server.

### ORDER BY push-down

`etcd_fdw` now also supports order by push-down. If possible, push order by
clause to the remote server so that we get the ordered result set from the
foreign server itself.

### LIMIT push-down

`etcd_fdw` now also supports limit offset push-down. Wherever possible,
perform LIMIT operations on the remote server.

#### WHERE push-down

`etcd_fdw` now supports WHERE clause push-down for simple key-based comparisons. Whenever possible, equality and range conditions are translated into etcd key scans, so filtering is done on the remote server.
Currently supported operators: `=`, `>=`, `>`, `<=`, `<`, `BETWEEN`, and `LIKE 'prefix%'`.
This behavior is consistent with the prefix, range_end, and key options in `CREATE FOREIGN TABLE`.

## Usage

### CREATE SERVER options

`etcd_fdw` accepts the following options via the `CREATE SERVER` command:

- **connstr** as *string*, requuired

  Connetion string for etcd server i.e. `127.0.0.1:2379`

- **ssl_key** as *string*, optional, no default

  The path name of the client private key file.

- **ssl_cert** as *string*, optional, no default

  The path name of the client public key certificate file.

- **ssl_ca** as *string*, optional, no default

  The path name of the Certificate Authority (CA) certificate
    file. This option, if used, must specify the same certificate used
    by the server.

- **ssl_servername** as *string*, optional, no default

   The domain name to use for verifying the server’s TLS certificate during the handshake.
   This value must match the Common Name (CN) or one of the Subject Alternative Names (SANs) in the server’s certificate.

- **username** as *string*, optional, no default

  Username to use when connecting to etcd.

- **password** as *string*, optional, no default

  Password to authenticate to the etcd server with.

- **connect_timeout** as *string*, optional, default = `10`

  Timeout in seconds for establishing the initial connection to the etcd server.

- **request_timeout** as *string*, optional, default = `30`

  Timeout in seconds to each request after the connection has been established.

### CREATE FOREIGN TABLE options

`etcd_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command.

- **rowid_column** as *string*, mandatory, no default

  Specifies which column should be treated as the unique row identifier.
  Usually set to key.

- **prefix** as *string*, optional, no default

  Restrict the scan to keys beginning with this prefix.
  If not provided, the FDW will fetch all keys from the etcd server

- **keys_only** as *string*, optional, default `false`

  If set to true, only the keys are fetched, not the values.
  Useful to reduce network overhead when values are not needed.

- **revision** as *string*, optional, default `0`

  Read key-value data at a specific etcd revision.
  If 0, the latest revision is used.

- **key** as *string*, optional, no default

  The starting key to fetch from etcd.

  This option defines the beginning of the range.
  If neither `prefix` nor `key` is specified, the FDW will default to `\0` (the lowest possible key).

- **range_end** as *string*, optional, no default

  The exclusive end of the key range. Restricts the scan to the half-open interval `[key, range_end)`.

  All keys between key (inclusive) and range_end (exclusive) will be returned.
  If range_end is omitted, only the single key defined by key will be returned (unless prefix is used).

- **consistency** as *string*, optional, default `l`

  Specifies the read consistency level for etcd queries.

  Linearizable(`l`), Ensures the result reflects the latest consensus state of the cluster.
  Linearizable reads have higher latency but guarantee fresh data.

  Serializable(`s`), Allows serving results from a local etcd member without cluster-wide consensus.
  Serializable reads are faster and lighter on the cluster, but may return stale data in some cases

## What doesn't work

etcd_fdw supports almost all kinds of CRUD operations. What doesn't work is modifying the key (which is the rowid value) directly using `UPDATE` statements.
What does work is the following workflow:

```
etcd_fdw=# SELECT * FROM test;
  key  | value
-------+-------
 bar   | baz
 foo   | abc
(2 rows)
etcd_fdw=# INSERT INTO TEST (key,value) SELECT key || '_new', value FROM test;
INSERT 0 2
etcd_fdw=# DELETE FROM test WHERE NOT key LIKE '%_new';
DELETE 2
etcd_fdw=# SELECT * FROM test;
    key    | value
-----------+-------
 bar_new   | baz
 foo_new   | abc
```
