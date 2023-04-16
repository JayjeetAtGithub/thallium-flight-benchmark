import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

if __name__ == "__main__":
    data = {
        "latency(s)": list(),
        "mode": list(),
        "selectivity": list()
    }

    filelist = [
        'flight',
        'thallium-1',
        'thallium-2',
        'thallium-3'
    ]
    for filename in filelist:
        with open(filename, "r") as fd:
            lines = fd.readlines()
            lines = [float(l.rstrip()) for l in lines]
            for l in lines[0:10]:
                data['latency(s)'].append(l)
                data['mode'].append(filename)
                data['selectivity'].append("100")

            for l in lines[10:20]:
                data['latency(s)'].append(l)
                data['mode'].append(filename)
                data['selectivity'].append("10")

            for l in lines[20:30]:
                data['latency(s)'].append(l)
                data['mode'].append(filename)
                data['selectivity'].append("1")

    df = pd.DataFrame(data)
    print(df)
    sns_plot = sns.barplot(x="mode", y="latency(s)", hue="selectivity", data=df)
    plt.title("Selectivity")
    plt.savefig(f'plot.pdf')
    plt.cla()
    plt.clf()