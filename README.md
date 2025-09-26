# etcd_fdw
A foreign data wrapper around etcd for postgres

## Setup
- Install pgrx on your machine `cargo install --locked cargo-pgrx --version 0.14.3`
- Setup pgrx `cargo pgrx init`
- Install protoc and protobuf (needed by etcd-client)
  - Instructions can be found [here](https://protobuf.dev/installation/)
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
CREATE foreign table test (key text, value text) server my_etcd_server options(rowid 'key');
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


Usage
-----

## CREATE SERVER options

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


## CREATE FOREIGN TABLE options

`etcd_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command.

- **rowid** as *string*, mandatory, no default

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

- **range** as *string*, optional, no default

  Restricts the scan to the half-open interval `[key, range)`.
  Example: with range `/gamma` and scan starting at `/`, the query will return keys strictly less than `/gamma`.


  Note: Cannot be used together with `prefix`.

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
