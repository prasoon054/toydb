# ToyDB

ToyDB is a simplified database system built in layers on top of a provided physical layer (`pflayer`) and an access method layer (`amlayer`).
This repository focuses on implementing the missing **record (tuple) layer** and testing it by loading CSV data, building indexes, and retrieving data.

---

## Problem Statement

### 1. Building a Record Layer

We implement the record/tuple layer (`dblayer`) on top of the page-based physical layer (`pflayer`).

* Each page is structured as a **slotted-page**:

  * **Header**: stores an array of slot offsets, number of records, and the free-space pointer.
  * **Records**: stored from the bottom of the page upwards.
* Records are addressed by a 4-byte **RID** (Record ID):

  * High 3 bytes: page number
  * Low 1 byte: slot number within the page

Tasks:

* Fill missing code in `tbl.c` and `tbl.h`.
* Handle record encoding/decoding in `loaddb.c` using helper functions in `codec.c`.

---

### 2. Testing: Loading CSV Data

We load CSV data into the database:

* Parse each row into fields.
* Encode each field into a record buffer.
* Insert into the table with `Table_Insert`, get an RID.
* Insert the RID into a BTree index using `AM_InsertEntry`.

CSV schema example:

```
country:varchar, population:int, capital:varchar
```

---

### 3. Testing: Retrieving Data

Two retrieval modes are supported via `dumpdb`:

* **Sequential scan** (`dumpdb s`): uses `Table_Scan` to iterate records page by page.
* **Index scan** (`dumpdb i`): uses the BTree index to fetch RIDs, then `Table_Get` to retrieve records.

Both methods decode the record back into CSV format for verification against the original data.

---

## Implementation Details

### `dblayer/tbl.c`

#### **Table\_Open**

* Opens or creates a DB file using `PF_OpenFile` / `PF_CreateFile`.
* Initializes the `Table` struct with schema and file descriptor.
* Overwrites existing DB file if requested.

#### **Table\_Close**

* Closes the paged file using `PF_CloseFile`.
* Frees the `Table` struct.

#### **Table\_Insert**

* Iterates pages using `PF_GetFirstPage` / `PF_GetNextPage`.
* Checks free space via slot count and free-space pointer.
* If space is insufficient, allocates a new page (`PF_AllocPage`).
* Copies record into free space, updates slot table and header.
* Returns RID = `(pageNum << 16) | slotNum`.

#### **Table\_Get**

* Fetches a record given its RID.
* Retrieves page using `PF_GetThisPage`.
* Uses slot offset + record length to copy record into buffer.
* Unfixes page and returns record length.

#### **Table\_Scan**

* Sequentially visits all pages using `PF_GetFirstPage` / `PF_GetNextPage`.
* For each page, iterates through slot table, fetches each record, and invokes the callback function (`callbackfn`).
* Useful for sequential scans (e.g., reconstructing CSV).

---

### `dblayer/loaddb.c`

#### **encode**

* Encodes a CSV row into a record buffer.
* Uses `codec.c` helpers for primitive types (`int`, `long`, `varchar`).
* Handles variable-length fields like strings (`varchar`).
* Produces a single record blob to be passed into `Table_Insert`.

---

### `dblayer/dumpdb.c`

#### **printRow**

* Decodes a record blob back into individual fields using `codec.c`.
* Prints the row in CSV format for output.

#### **indexScan**

* Opens the BTree index via AM-layer functions.
* Iterates through all index entries, fetching RIDs.
* Calls `Table_Get` for each RID and prints the decoded row.
* Ensures correctness by comparing against sequential scan results.

---

## Miscellaneous Notes

1. **Documentation**: Refer to `am.pdf` and `pf.pdf` for existing access method and physical layer details. Internal mechanics are not required.
2. **Build Order**: Always build `pflayer` and `amlayer` before `dblayer`.
3. **Testing**: Use the provided `run_test.sh` script to build and test automatically.
