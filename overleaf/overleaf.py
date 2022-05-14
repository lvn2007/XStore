import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

fileList = ['ss-sc_1000_gRPC.csv', 'ss-sc_1000_Protobuf.csv', 'ss-sc_1000_Raw.csv', 'zmq_1000_Protobuf-ZMQ.csv']

grpcDF = pd.read_csv(fileList[0], skiprows=2, nrows=999, names=['Time taken (ms)'])
tcpProtobufDF = pd.read_csv(fileList[1], skiprows=2, nrows=999, names=['Time taken (ms)'])
tcpRawDF = pd.read_csv(fileList[2], skiprows=2, nrows=999, names=['Time taken (ms)'])
zmqDF = pd.read_csv(fileList[3], skiprows=2, nrows=999, names=['Time taken (us)'])

# Convert ZMQ usec to msec
zmqDF['Time taken (us)'] = zmqDF['Time taken (us)'] / 1000
zmqDF.columns = ['Time taken (ms)']

# Mean calc
grpcMean = [np.mean(grpcDF['Time taken (ms)'])] * len(grpcDF)
tcpProtobufMean = [np.mean(tcpProtobufDF['Time taken (ms)'])] * len(grpcDF)
tcpRawMean = [np.mean(tcpRawDF['Time taken (ms)'])] * len(grpcDF)
zmqMean = [np.mean(zmqDF['Time taken (ms)'])] * len(grpcDF)

# Axis
fig, ax = plt.subplots()

# Plot main line
ax.plot(grpcDF.index, grpcDF['Time taken (ms)'], label='gRPC', linewidth=1)
ax.plot(grpcDF.index, tcpProtobufDF['Time taken (ms)'], label='TCP with Protobuf', linewidth=1)
ax.plot(grpcDF.index, tcpRawDF['Time taken (ms)'], label='TCP Byte', linewidth=1)
ax.plot(grpcDF.index, zmqDF['Time taken (ms)'], label='ZeroMQ', linewidth=1)

# Plot mean line
ax.plot(grpcDF.index, grpcMean, linestyle='--', linewidth=1)
ax.plot(grpcDF.index, tcpProtobufMean, linestyle='--', linewidth=1)
ax.plot(grpcDF.index, tcpRawMean, linestyle='--', linewidth=1)
ax.plot(grpcDF.index, zmqMean, linestyle='--', linewidth=1)

# Labels & Legends
ax.set_xlabel('Iteration')
ax.set_ylabel('Latency (ms)')
plt.legend()
plt.tight_layout()

# plt.show()
plt.savefig('latencyBench.svg', format='svg')