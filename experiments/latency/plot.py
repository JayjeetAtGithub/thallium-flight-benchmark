import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

if __name__ == "__main__":
    data = {
        "latency": list(),
        "mode": list()
    }

    with open("thallium.out.3000", "r") as th_file:
        lines = th_file.readlines()
        lines = [float(l.rstrip()) for l in lines]
        for l in lines:
            data['latency'].append(l)
            data['mode'].append('thallium')


                
    with open("flight.out.3000", "r") as flight_file:
        lines = flight_file.readlines()
        lines = [float(l.rstrip()) for l in lines]
        for l in lines:
            data['latency'].append(l)
            data['mode'].append('flight')


    df = pd.DataFrame(data)
    print(df)
    sns_plot = sns.barplot(x="mode", y="latency", data=df)
    plt.title("Latency: 311.7 GB")
    plt.savefig('latency_3000.pdf')
