import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

if __name__ == "__main__":
    data = {
        "latency(s)": list(),
        "mode": list()
    }

    filelist = [
        "skyhook-cephfs",
        "flight-ext4",
        "thallium-ext4",
        "thallium-ext4mmap",
        "thallium-bake",
        "flight-ext4dataset",
        "thallium-ext4dataset",
        "flight-transport",
        "thallium-transport"
    ]

    for filename in filelist:
        with open(filename, "r") as fd:
            lines = fd.readlines()
            lines = [float(l.rstrip()) for l in lines]
            for l in lines:
                data['latency(s)'].append(l)
                data['mode'].append(filename)
                
    df = pd.DataFrame(data)
    print(df)
    sns_plot = sns.barplot(x="mode", y="latency(s)", data=df)
    plt.title("Mochi/Flight/Skyhook")
    plt.xticks(rotation=12)
    plt.savefig('plot.pdf')
