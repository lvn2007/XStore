SITE_NAME="$1"
FILE_NAME="10Y_128-Cols_Experiments.tar.zst"
FOLDER_NAME=${FILE_NAME%.tar.zst}

urlTACC="https://chi.tacc.chameleoncloud.org:7480/swift/v1/AUTH_07e3c8c4081143c189d399c676ad139f/XStore_ExperimentDataset/${FILE_NAME}"
urlMystic="http://172.16.6.231:8000/XStore_ExperimentDataset/${FILE_NAME}"

# Default download URL to TACC site
defaultURL=${urlTACC}

if [ "$SITE_NAME" == "CHI@TACC" ]; then
    defaultURL=${urlTACC}
elif [ "$SITE_NAME" == "MYSTIC@IIT" ]; then
    defaultURL=${urlMystic}
fi

# Preemptively remove old stuffs
rm -rf ${FOLDER_NAME} ${FILE_NAME}*

# Fetch experiment data from ObjectStore
wget -q ${defaultURL}

# Deflate compressed dataset
tar -xvSf ${FILE_NAME} && rm ${FILE_NAME}