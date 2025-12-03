open Printf
open Sqlite3

let assert_ok rc = assert (rc = Rc.OK)
let assert_done rc = assert (rc = Rc.DONE)

(* Helper to create a blob of specified size with deterministic content *)
let make_blob size = String.init size (fun i -> Char.chr (i mod 256))

(* Helper to compare bigarray content with expected string *)
let compare_bigarray_to_string ba str len =
  let ba_len = Bigarray.Array1.dim ba in
  if len > ba_len || len > String.length str then false
  else
    let rec loop i =
      if i >= len then true
      else if Bigarray.Array1.get ba i <> String.get str i then false
      else loop (i + 1)
    in
    loop 0

let%test "test_blob_basic" =
  printf "Testing basic blob read into bigarray...\n%!";

  (* Create in-memory database *)
  let db = db_open ":memory:" in

  (* Create table with blob column *)
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert a small blob *)
  let blob_data = "Hello, World!" in
  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_blob stmt 2 blob_data);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Read blob into bigarray *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 1024 in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L buffer in

  printf "  Read %d bytes (expected %d)\n%!" bytes_read
    (String.length blob_data);
  assert (bytes_read = String.length blob_data);
  assert (compare_bigarray_to_string buffer blob_data bytes_read);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_exact_buffer_size" =
  printf "Testing blob read with exact buffer size...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert blob of known size *)
  let blob_data = make_blob 256 in
  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_blob stmt 2 blob_data);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Read with exact size buffer *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 256 in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L buffer in

  printf "  Read %d bytes (expected 256)\n%!" bytes_read;
  assert (bytes_read = 256);
  assert (compare_bigarray_to_string buffer blob_data 256);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_larger_buffer" =
  printf "Testing blob read with buffer larger than blob...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert small blob *)
  let blob_data = make_blob 100 in
  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_blob stmt 2 blob_data);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Read with larger buffer *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 1024 in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L buffer in

  printf "  Read %d bytes (expected 100)\n%!" bytes_read;
  assert (bytes_read = 100);
  assert (compare_bigarray_to_string buffer blob_data 100);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_smaller_buffer" =
  printf "Testing blob read with buffer smaller than blob (truncation)...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert large blob *)
  let blob_data = make_blob 1000 in
  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_blob stmt 2 blob_data);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Read with smaller buffer - should truncate *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 256 in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L buffer in

  printf "  Read %d bytes (expected 256, blob was 1000)\n%!" bytes_read;
  assert (bytes_read = 256);
  assert (compare_bigarray_to_string buffer blob_data 256);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_multiple_rows" =
  printf "Testing blob read from multiple rows...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert multiple blobs *)
  let blobs = [| make_blob 50; make_blob 100; make_blob 200; make_blob 500 |] in

  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  Array.iteri
    (fun i blob_data ->
      assert_ok (reset stmt);
      assert_ok (bind_int64 stmt 1 (Int64.of_int (i + 1)));
      assert_ok (bind_blob stmt 2 blob_data);
      assert_done (step stmt))
    blobs;
  assert_ok (finalize stmt);

  (* Read each blob and verify *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 1024 in
  Array.iteri
    (fun i expected ->
      let rowid = Int64.of_int (i + 1) in
      let bytes_read = blob_read_into_bigarray db "blobs" "data" rowid buffer in
      printf "  Row %Ld: read %d bytes (expected %d)\n%!" rowid bytes_read
        (String.length expected);
      assert (bytes_read = String.length expected);
      assert (compare_bigarray_to_string buffer expected bytes_read))
    blobs;

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_binary_data" =
  printf "Testing blob read with binary data (all byte values)...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Create blob with all possible byte values *)
  let blob_data = String.init 256 (fun i -> Char.chr i) in
  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_blob stmt 2 blob_data);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Read and verify all bytes *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 256 in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L buffer in

  printf "  Read %d bytes containing all byte values 0-255\n%!" bytes_read;
  assert (bytes_read = 256);

  (* Verify each byte value *)
  for i = 0 to 255 do
    let expected = Char.chr i in
    let actual = Bigarray.Array1.get buffer i in
    if actual <> expected then
      failwith
        (sprintf "Byte %d mismatch: expected %d, got %d" i (Char.code expected)
           (Char.code actual))
  done;

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_empty" =
  printf "Testing blob read with empty blob...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert empty blob *)
  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_blob stmt 2 "");
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Read empty blob *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 1024 in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L buffer in

  printf "  Read %d bytes (expected 0 for empty blob)\n%!" bytes_read;
  assert (bytes_read = 0);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_nonexistent_row" =
  printf "Testing blob read from nonexistent row...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Don't insert anything, try to read *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 1024 in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 999L buffer in

  printf "  Read %d bytes (expected 0 for nonexistent row)\n%!" bytes_read;
  assert (bytes_read = 0);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_large" =
  printf "Testing blob read with large blob (1MB)...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert 1MB blob *)
  let size = 1024 * 1024 in
  let blob_data = make_blob size in
  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_blob stmt 2 blob_data);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Read with exact size buffer *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout size in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L buffer in

  printf "  Read %d bytes (expected %d)\n%!" bytes_read size;
  assert (bytes_read = size);
  assert (compare_bigarray_to_string buffer blob_data size);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_reuse_buffer" =
  printf "Testing blob read with buffer reuse...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert two different blobs *)
  let blob1 = String.make 100 'A' in
  let blob2 = String.make 50 'B' in

  let stmt = prepare db "INSERT INTO blobs (id, data) VALUES (?, ?)" in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_blob stmt 2 blob1);
  assert_done (step stmt);
  assert_ok (reset stmt);
  assert_ok (bind_int64 stmt 1 2L);
  assert_ok (bind_blob stmt 2 blob2);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Reuse same buffer for multiple reads *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 1024 in

  (* Read first blob *)
  let bytes_read1 = blob_read_into_bigarray db "blobs" "data" 1L buffer in
  printf "  First read: %d bytes\n%!" bytes_read1;
  assert (bytes_read1 = 100);
  assert (compare_bigarray_to_string buffer blob1 100);

  (* Read second blob into same buffer *)
  let bytes_read2 = blob_read_into_bigarray db "blobs" "data" 2L buffer in
  printf "  Second read: %d bytes\n%!" bytes_read2;
  assert (bytes_read2 = 50);
  assert (compare_bigarray_to_string buffer blob2 50);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_null_value" =
  printf "Testing blob read from NULL column...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Insert row with NULL blob *)
  assert_ok (exec db "INSERT INTO blobs (id, data) VALUES (1, NULL)");

  (* Try to read NULL blob *)
  let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 1024 in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L buffer in

  printf "  Read %d bytes (expected 0 for NULL)\n%!" bytes_read;
  assert (bytes_read = 0);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

(* ===== Blob Write Tests ===== *)

(* Helper to fill bigarray with deterministic content *)
let fill_bigarray ba len =
  for i = 0 to len - 1 do
    Bigarray.Array1.set ba i (Char.chr (i mod 256))
  done

(* Helper to compare two bigarrays byte-by-byte *)
let compare_bigarrays ba1 ba2 len =
  let rec loop i =
    if i >= len then true
    else if Bigarray.Array1.get ba1 i <> Bigarray.Array1.get ba2 i then (
      printf "  Mismatch at byte %d: expected %d, got %d\n%!" i
        (Char.code (Bigarray.Array1.get ba1 i))
        (Char.code (Bigarray.Array1.get ba2 i));
      false)
    else loop (i + 1)
  in
  loop 0

let%test "test_blob_write_basic" =
  printf "Testing basic blob write from bigarray...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Create source bigarray with test data *)
  let size = 100 in
  let src_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
  in
  fill_bigarray src_buffer size;

  (* Insert row with zeroblob placeholder *)
  let stmt =
    prepare db "INSERT INTO blobs (id, data) VALUES (?, zeroblob(?))"
  in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_int stmt 2 size);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Write data using blob_write_from_bigarray *)
  let success = blob_write_from_bigarray db "blobs" "data" 1L src_buffer size in
  assert success;

  (* Read back into different buffer and compare *)
  let dst_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
  in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L dst_buffer in

  printf "  Wrote %d bytes, read back %d bytes\n%!" size bytes_read;
  assert (bytes_read = size);
  assert (compare_bigarrays src_buffer dst_buffer size);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_write_multiple" =
  printf "Testing multiple blob writes...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  let sizes = [| 50; 100; 256; 1000; 4096 |] in

  (* Write multiple blobs *)
  Array.iteri
    (fun i size ->
      let rowid = Int64.of_int (i + 1) in

      (* Create source data with unique pattern for each blob *)
      let src_buffer =
        Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
      in
      for j = 0 to size - 1 do
        Bigarray.Array1.set src_buffer j (Char.chr ((j + (i * 17)) mod 256))
      done;

      (* Insert with zeroblob *)
      let stmt =
        prepare db "INSERT INTO blobs (id, data) VALUES (?, zeroblob(?))"
      in
      assert_ok (bind_int64 stmt 1 rowid);
      assert_ok (bind_int stmt 2 size);
      assert_done (step stmt);
      assert_ok (finalize stmt);

      (* Write blob data *)
      let success =
        blob_write_from_bigarray db "blobs" "data" rowid src_buffer size
      in
      assert success)
    sizes;

  (* Read back all blobs and verify *)
  Array.iteri
    (fun i size ->
      let rowid = Int64.of_int (i + 1) in

      (* Recreate expected data *)
      let expected =
        Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
      in
      for j = 0 to size - 1 do
        Bigarray.Array1.set expected j (Char.chr ((j + (i * 17)) mod 256))
      done;

      (* Read and compare *)
      let dst_buffer =
        Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
      in
      let bytes_read =
        blob_read_into_bigarray db "blobs" "data" rowid dst_buffer
      in

      printf "  Blob %d: wrote %d bytes, read %d bytes\n%!" (i + 1) size
        bytes_read;
      assert (bytes_read = size);
      assert (compare_bigarrays expected dst_buffer size))
    sizes;

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_write_large" =
  printf "Testing large (1MB) blob write...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Test with 1MB blob *)
  let size = 1024 * 1024 in
  let src_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
  in
  fill_bigarray src_buffer size;

  (* Insert with zeroblob *)
  let stmt =
    prepare db "INSERT INTO blobs (id, data) VALUES (?, zeroblob(?))"
  in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_int stmt 2 size);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Write blob data *)
  let success = blob_write_from_bigarray db "blobs" "data" 1L src_buffer size in
  assert success;

  (* Read back and compare *)
  let dst_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
  in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L dst_buffer in

  printf "  Wrote %d bytes, read back %d bytes\n%!" size bytes_read;
  assert (bytes_read = size);
  assert (compare_bigarrays src_buffer dst_buffer size);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_write_partial" =
  printf "Testing partial blob write...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Create larger buffer but only write part of it *)
  let buffer_size = 1000 in
  let write_size = 500 in
  let src_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout buffer_size
  in
  fill_bigarray src_buffer buffer_size;

  (* Insert with zeroblob of write_size *)
  let stmt =
    prepare db "INSERT INTO blobs (id, data) VALUES (?, zeroblob(?))"
  in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_int stmt 2 write_size);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Write only write_size bytes *)
  let success =
    blob_write_from_bigarray db "blobs" "data" 1L src_buffer write_size
  in
  assert success;

  (* Read back and compare only the written portion *)
  let dst_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout write_size
  in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L dst_buffer in

  printf "  Wrote %d bytes from %d-byte buffer, read back %d bytes\n%!"
    write_size buffer_size bytes_read;
  assert (bytes_read = write_size);
  assert (compare_bigarrays src_buffer dst_buffer write_size);

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_write_binary_data" =
  printf "Testing blob write with all byte values (0-255)...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  (* Create buffer with all 256 byte values *)
  let size = 256 in
  let src_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
  in
  for i = 0 to 255 do
    Bigarray.Array1.set src_buffer i (Char.chr i)
  done;

  (* Insert with zeroblob *)
  let stmt =
    prepare db "INSERT INTO blobs (id, data) VALUES (?, zeroblob(?))"
  in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_int stmt 2 size);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  (* Write blob data *)
  let success = blob_write_from_bigarray db "blobs" "data" 1L src_buffer size in
  assert success;

  (* Read back and verify each byte *)
  let dst_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
  in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L dst_buffer in

  printf "  Wrote and read %d bytes\n%!" bytes_read;
  assert (bytes_read = size);
  for i = 0 to 255 do
    let expected = Char.chr i in
    let actual = Bigarray.Array1.get dst_buffer i in
    if actual <> expected then
      failwith
        (sprintf "Byte %d mismatch: expected %d, got %d" i (Char.code expected)
           (Char.code actual))
  done;

  assert (db_close db);
  printf "  PASSED\n%!";
  true

let%test "test_blob_write_overwrite" =
  printf "Testing blob overwrite...\n%!";

  let db = db_open ":memory:" in
  assert_ok (exec db "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)");

  let size = 100 in

  (* Write first version *)
  let src1 = Bigarray.Array1.create Bigarray.char Bigarray.c_layout size in
  for i = 0 to size - 1 do
    Bigarray.Array1.set src1 i 'A'
  done;

  let stmt =
    prepare db "INSERT INTO blobs (id, data) VALUES (?, zeroblob(?))"
  in
  assert_ok (bind_int64 stmt 1 1L);
  assert_ok (bind_int stmt 2 size);
  assert_done (step stmt);
  assert_ok (finalize stmt);

  let success = blob_write_from_bigarray db "blobs" "data" 1L src1 size in
  assert success;

  (* Overwrite with different data (same size) *)
  let src2 = Bigarray.Array1.create Bigarray.char Bigarray.c_layout size in
  for i = 0 to size - 1 do
    Bigarray.Array1.set src2 i 'B'
  done;

  let success = blob_write_from_bigarray db "blobs" "data" 1L src2 size in
  assert success;

  (* Read back and verify it's the second version *)
  let dst_buffer =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout size
  in
  let bytes_read = blob_read_into_bigarray db "blobs" "data" 1L dst_buffer in

  printf "  Overwrote blob, read back %d bytes\n%!" bytes_read;
  assert (bytes_read = size);
  assert (compare_bigarrays src2 dst_buffer size);

  assert (db_close db);
  printf "  PASSED\n%!";
  true
