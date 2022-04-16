import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

toAnalyze = ['ss-sc_1000_Protobuf.csv', 'zmq_1000_Protobuf-ZMQ.csv']

for data in toAnalyze:
    df = pd.read_csv(data, header=0, skiprows=[1])

    fig, ax = plt.subplots()

    x_axis = df.index.values
    y_axis = df['Time taken (us)']

    # Plot data
    dataLine = ax.plot(x_axis, y_axis, label='Time taken (us)')

    #   Mean line
    currMean = [np.mean(df['Time taken (us)'])] * len(x_axis)
    ax.plot(x_axis, currMean, label=f'Mean: {currMean[0]:,.5f}', linestyle='--')

    #   Min line
    currMin = [np.min(df['Time taken (us)'])] * len(x_axis)
    ax.plot(x_axis, currMin, label=f'Min: {currMin[0]:,.5f}', linestyle='--')

    #   Max line
    currMax = [np.max(df['Time taken (us)'])] * len(x_axis)
    ax.plot(x_axis, currMax, label=f'Max: {currMax[0]:,.5f}', linestyle='--')

    #   Legend setting
    legend = ax.legend(loc='upper right')

    plt.title(f'1 Server - 1 Server | {data.split("_")[-1].strip(".csv")}')
    plt.savefig(f'{data.replace(".csv", "")}.png')
    print(f'{data.replace(".csv", "")}.png')
    # plt.show()