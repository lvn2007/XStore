serverNthreads=$1
nClients=$2

echo "--- $serverNthreads Server ---"
echo "--- Parallel Clients: $nClients ---"

# Remove all existing .csv files
rm -r *.csv

# Running N clients in parallel
cmd="parallel --jobs $nClients ./xstoreClient_Protobuf {#} ::: {1..$nClients}"
eval $cmd

python3 analyze.py $serverNthreads $nClients