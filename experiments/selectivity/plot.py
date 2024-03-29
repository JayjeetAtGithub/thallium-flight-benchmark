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

    # filelist = [
    #     "file_flight",
    #     "file_tl",
    #     "bake_tl"
    # ]

    filelist = [
        'fl_tcp_grpc',
        'tl_ofi_tcp',
        'tl_ofi_verbs',
        'tl_ucx_all',
        'tl_ucx_tcp'
    ]

    for filename in filelist:
        with open(filename, "r") as fd:
            lines = fd.readlines()
            lines = [float(l.rstrip()) for l in lines]
            for l in lines[0:5]:
                data['latency(s)'].append(l)
                data['mode'].append(filename)
                data['selectivity'].append("100")

            for l in lines[5:10]:
                data['latency(s)'].append(l)
                data['mode'].append(filename)
                data['selectivity'].append("10")

            for l in lines[10:15]:
                data['latency(s)'].append(l)
                data['mode'].append(filename)
                data['selectivity'].append("1")
                
    df = pd.DataFrame(data)
    print(df)
    sns_plot = sns.barplot(x="mode", y="latency(s)", hue="selectivity", data=df)
    plt.title("Selectivity")
    plt.xticks(rotation=20)

    plt.savefig('plot2.pdf')
