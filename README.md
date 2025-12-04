# SQLite3-OCaml Fork with BLOB Streaming

A fork of [sqlite3-ocaml](https://github.com/mmottl/sqlite3-ocaml) that adds incremental BLOB I/O.

## Why?

The original library can only read BLOBs by copying them entirely into OCaml strings. For an application handling many image thumbnails, this creates unnecessary memory pressure.

This fork exposes SQLite's [incremental BLOB API](https://www.sqlite.org/c3ref/blob_open.html), allowing you to read and write BLOB data directly from/to Bigarray buffers—no intermediate copies.

## New Functions

```ocaml
val blob_open  : db -> table:string -> column:string -> row:int64 -> write:bool -> blob
val blob_read  : blob -> buf:buffer -> offset:int -> len:int -> unit
val blob_write : blob -> buf:buffer -> offset:int -> len:int -> unit
val blob_close : blob -> unit
val blob_bytes : blob -> int
```

## Build

```bash
dune build
```

## Test

```bash
dune runtest
```

## Example

```ocaml
let db = Sqlite3.db_open "test.db" in

(* Read a JPEG thumbnail directly into a buffer *)
let blob = Sqlite3.blob_open db ~table:"thumbs" ~column:"jpg" ~row:1L ~write:false in
let size = Sqlite3.blob_bytes blob in
let buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout size in
Sqlite3.blob_read blob ~buf ~offset:0 ~len:size;
Sqlite3.blob_close blob
(* buf now contains the JPEG data *)
```

## Upstream

Original: https://github.com/mmottl/sqlite3-ocaml
