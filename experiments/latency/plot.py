import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

if __name__ == "__main__":
    data = {
        "latency": list(),
        "mode": list()
    }

    with open("thallium.out", "r") as th_file:
        lines = th_file.readlines()
        lines = [float(l.rstrip()) for l in lines]
        for l in lines:
            data['latency'].append(l)
            data['mode'].append('thallium')


                
    with open("flight.out", "r") as flight_file:
        lines = flight_file.readlines()
        lines = [float(l.rstrip()) for l in lines]
        for l in lines:
            data['latency'].append(l)
            data['mode'].append('flight')


    df = pd.DataFrame(data)
    print(df)
    sns_plot = sns.barplot(x="mode", y="latency", data=df)
    plt.title("Latency: 207.8 GB")
    plt.savefig('latency.pdf')
