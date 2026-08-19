DB_NAME="$1"
UCASE_DB_NAME=""

if [ "$DB_NAME" == "XStore" ]; then
    UCASE_DB_NAME="XSTORE"
elif [ "$DB_NAME" == "MongoDB" ]; then
    UCASE_DB_NAME="MONGODB"
elif [ "$DB_NAME" == "InfluxDB" ]; then
    UCASE_DB_NAME="INFLUXDB"
elif [ "$DB_NAME" == "Graphite" ]; then
    UCASE_DB_NAME="GRAPHITE"    
fi

for DIR in INSERT_UNARY_SEQ INSERT_UNARY_RAND INSERT_BATCH_SEQ INSERT_BATCH_RAND QUERY_UNARY_SEQ QUERY_UNARY_RAND QUERY_BATCH_SEQ QUERY_BATCH_RAND AGG_MIN_QUERY AGG_MAX_QUERY AGG_AVG_QUERY AGG_SUM_QUERY
do
    for clientNum in 1 2 4 8 16 32 64
    do
	targetDir="${DB_NAME}/${UCASE_DB_NAME}_${DIR}/Client-${clientNum}/Node-0"
        cat ../${targetDir}/cpu.txt | grep -v Average | grep -v Linux |awk '{if ($0 ~ /[0-9]/) { print $1","$2","$3","$4","$5","$6","$7","$8; }  }' > ../${targetDir}/cpuCleaned.txt
	cat ../${targetDir}/mem.txt | grep -v Average | grep -v Linux |awk '{if ($0 ~ /[0-9]/) { print $1","$2","$3","$4","$5","$6","$7","$8","$9","$10","$11","$12; }  }' > ../${targetDir}/memCleaned.txt
	cat ../${targetDir}/io.txt | grep -v Average | grep -v Linux |awk '{if ($0 ~ /[0-9]/) { print $1","$2","$3","$4","$5","$6","$7","$8; }  }' > ../${targetDir}/ioCleaned.txt
    done
done
