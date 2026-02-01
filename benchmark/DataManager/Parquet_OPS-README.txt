Specs:
- [1, 2, 4, 8, 16, 32] threads each node
- 8 client nodes

---- QUERY ----
UNARY_QUERY_SEQ:
- python3 Parquet_OPS.py queryWorkload -t UNARY_SEQ -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0
- python3 Parquet_OPS.py queryWorkload -t UNARY_SEQ -o 32000 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0
- python3 Parquet_OPS.py queryWorkload -t UNARY_SEQ -o 64000 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0

UNARY_QUERY_RAND: Same cmd for all client nodes
- python3 Parquet_OPS.py queryWorkload -t UNARY_RAND -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0

BATCH_QUERY_SEQ:
- python3 Parquet_OPS.py queryWorkload -t BATCH_SEQ -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10
- python3 Parquet_OPS.py queryWorkload -t BATCH_SEQ -o 320000 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10
- python3 Parquet_OPS.py queryWorkload -t BATCH_SEQ -o 640000 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10

BATCH_QUERY_RAND: Same cmd for all client nodes
- python3 Parquet_OPS.py queryWorkload -t BATCH_RAND -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10

---- INSERT ----
UNARY_INSERT_SEQ:
- python3 Parquet_OPS.py insertWorkload -t UNARY_SEQ -o 0 -s 1000 -n 32 -i hashData.parquet -b 0
- python3 Parquet_OPS.py insertWorkload -t UNARY_SEQ -o 32000 -s 1000 -n 32 -i hashData.parquet -b 0
- python3 Parquet_OPS.py insertWorkload -t UNARY_SEQ -o 64000 -s 1000 -n 32 -i hashData.parquet -b 0

UNARY_INSERT_RAND: Same cmd for all client nodes
- python3 Parquet_OPS.py insertWorkload -t UNARY_RAND -o 0 -s 1000 -n 32 -i hashData.parquet -b 0

BATCH_INSERT_SEQ:
- python3 Parquet_OPS.py insertWorkload -t BATCH_SEQ -o 0 -s 1000 -n 32 -i hashData.parquet -b 10
- python3 Parquet_OPS.py insertWorkload -t BATCH_SEQ -o 320000 -s 1000 -n 32 -i hashData.parquet -b 10
- python3 Parquet_OPS.py insertWorkload -t BATCH_SEQ -o 640000 -s 1000 -n 32 -i hashData.parquet -b 10

BATCH_INSERT_RAND: Same cmd for all client nodes
- python3 Parquet_OPS.py insertWorkload -t BATCH_RAND -o 0 -s 1000 -n 32 -i hashData.parquet -b 10