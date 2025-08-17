#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== Building pflayer ==="
cd pflayer
make clean && make

echo "=== Building amlayer ==="
cd ../amlayer
make clean && make

echo "=== Building dblayer ==="
cd ../dblayer
make clean && make

echo "=== Cleaning old DB/index/output files ==="
rm -f data.db data.db.* out_seq.csv out_idx.csv loaddb.out dumpdb_seq.out dumpdb_idx.out seq_sorted.csv idx_sorted.csv

echo "=== Task 2: Loading CSV into DB and creating index ==="
./loaddb | tee loaddb.out

echo "=== Task 3a: Sequential Scan ==="
./dumpdb s | tee dumpdb_seq.out > out_seq.csv
echo "--- Comparing index scan output with original (order-independent) ---"
sort out_seq.csv > seq_sorted.csv
diff -u data.csv out_seq.csv && echo "Sequential-scan output matches original"

echo "=== Task 3b: Index Scan ==="
./dumpdb i | tee dumpdb_idx.out > out_idx.csv
echo "--- Comparing index scan output with original (order-independent) ---"
sort out_idx.csv > idx_sorted.csv
diff -u data.csv idx_sorted.csv && echo "Index-scan output matches original (as sets)"

echo "=== All tasks completed successfully ==="

echo "=== Cleanup ==="
rm dumpdb_idx.out dumpdb_seq.out loaddb.out idx_sorted.csv  seq_sorted.csv
cd ../dblayer
make clean
cd ../amlayer
make clean
cd ../pflayer
make clean
