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