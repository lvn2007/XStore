import sys
import pandas as pd
import matplotlib.pyplot as plt
import glob
import numpy as np

allCSV = glob.glob('*.csv')

serverNthreads = sys.argv[1]
nClients = sys.argv[2]

# Read master
df = pd.read_csv(allCSV[0], skiprows=2, nrows=999, names=['Time taken (ms)'])

# Loop through each available files and append to master
for i in range(1, len(allCSV)):
    tempDF = pd.read_csv(allCSV[i], skiprows=2, nrows=999, names=['Time taken (ms)'])
    df[f'{i}'] = tempDF['Time taken (ms)']
print(df)

df['Composite'] = df.mean(axis=1)
print(df)

# Mean calc
dfMean = [np.mean(df['Composite'])] * len(df)

# # Axis
fig, ax = plt.subplots()

# Plot main line
ax.plot(df.index, df['Composite'], label=f'{nClients} Parallel Clients', linewidth=1, color='g')

# Plot mean line
ax.plot(df.index, dfMean, linestyle='--', linewidth=1, label=f'Mean: {df["Composite"].mean():,.2f}', color='red')

# Labels & Legends
ax.set_title(f'{serverNthreads}-Threads SERVER -- {nClients} Parallel Clients')
ax.set_xlabel('Iteration')
ax.set_ylabel('Latency (ms)')
plt.legend()

plt.show()
plt.savefig(f'{serverNthreads}-threadsServer_{nClients}parallelClients.svg', format='svg')

# Dump this run data to csv
df.to_csv(f'{serverNthreads}-threadsServer_{nClients}parallelClients.csv', index=None)