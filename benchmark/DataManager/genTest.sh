# Making folder
for i in `seq 0 7`; do mkdir -p UNARY_QUERY_SEQ/UNARY_QUERY_SEQ_Node-$i; done
for i in `seq 0 7`; do mkdir -p UNARY_QUERY_RAND/UNARY_QUERY_RAND_Node-$i; done
for i in `seq 0 7`; do mkdir -p BATCH_QUERY_SEQ/BATCH_QUERY_SEQ_Node-$i; done
for i in `seq 0 7`; do mkdir -p BATCH_QUERY_RAND/BATCH_QUERY_RAND_Node-$i; done
for i in `seq 0 7`; do mkdir -p UNARY_INSERT_SEQ/UNARY_INSERT_SEQ_Node-$i; done
for i in `seq 0 7`; do mkdir -p UNARY_INSERT_RAND/UNARY_INSERT_RAND_Node-$i; done
for i in `seq 0 7`; do mkdir -p BATCH_INSERT_SEQ/BATCH_INSERT_SEQ_Node-$i; done
for i in `seq 0 7`; do mkdir -p BATCH_INSERT_RAND/BATCH_INSERT_RAND_Node-$i; done

# Generate data
for i in `seq 0 7`; do cd UNARY_QUERY_SEQ/UNARY_QUERY_SEQ_Node-$i; pwd; python3 ../../../Parquet_OPS.py queryWorkload -t UNARY_SEQ -o $((i * 32000)) -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0 ; cd ../..; done
for i in `seq 0 7`; do cd UNARY_QUERY_RAND/UNARY_QUERY_RAND_Node-$i; pwd; python3 ../../../Parquet_OPS.py queryWorkload -t UNARY_RAND -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0 ; cd ../..; done
for i in `seq 0 7`; do cd BATCH_QUERY_SEQ/BATCH_QUERY_SEQ_Node-$i; pwd; python3 ../../../Parquet_OPS.py queryWorkload -t BATCH_SEQ -o 0 -s 100 -n 32 -st 946684800 -et 1262303999 -b 1000 ; cd ../..; done
for i in `seq 0 7`; do cd BATCH_QUERY_RAND/BATCH_QUERY_RAND_Node-$i; pwd; python3 ../../../Parquet_OPS.py queryWorkload -t BATCH_RAND -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10 ; cd ../..; done

for i in `seq 0 7`; do cd UNARY_INSERT_SEQ/UNARY_INSERT_SEQ_Node-$i; pwd; python3 ../../../Parquet_OPS.py insertWorkload -t UNARY_SEQ -o $((i * 32000)) -s 1000 -n 32 -i ../../../hashData_SMALL.parquet -b 0 ; cd ../..; done
for i in `seq 0 7`; do cd UNARY_INSERT_RAND/UNARY_INSERT_RAND_Node-$i; pwd; python3 ../../../Parquet_OPS.py insertWorkload -t UNARY_RAND -o 0 -s 1000 -n 32 -i ../../../hashData_SMALL.parquet -b 0 ; cd ../..; done
for i in `seq 0 7`; do cd BATCH_INSERT_SEQ/BATCH_INSERT_SEQ_Node-$i; pwd; python3 ../../../Parquet_OPS.py insertWorkload -t BATCH_SEQ -o $((i * 320000)) -s 1000 -n 32 -i ../../../hashData_SMALL.parquet -b 10 ; cd ../..; done
for i in `seq 0 7`; do cd BATCH_INSERT_RAND/BATCH_INSERT_RAND_Node-$i; pwd; python3 ../../../Parquet_OPS.py insertWorkload -t BATCH_RAND -o 0 -s 1000 -n 32 -i ../../../hashData_SMALL.parquet -b 10 ; cd ../..; done