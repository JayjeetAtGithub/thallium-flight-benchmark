import os
import matplotlib.pyplot as plt

if __name__ == "__main__":
    selectivity = ["1", "10", "100"]
    mode = ["client", "server"]
    benchmark = ["0", "2", "2-fix"]

    for b in benchmark:
        for s in selectivity:
            data_dict = {}
            x = []
            y = []
            for m in mode:
                path = b + "/" + "result_" + m + "_" + s + ".txt"
                print(path)
                if os.path.exists(path):
                    with open(path, "r") as f:
                        lines = f.readlines()
                        for line in lines[:-1]:
                            key = str(line.split(":")[0].strip())
                            val = float(line.split(":")[1].strip())
                            if key not in data_dict:
                                data_dict[key] = []
                            data_dict[key].append(val)
            for key in data_dict:
                data_dict[key] = sum(data_dict[key])
            print(data_dict)
            plt.bar(data_dict.keys(), data_dict.values())
            plt.ylim(0, 12000)
            plt.ylabel("Time (ms)")
            plt.xticks(rotation = 15)
            plt.title("Benchmark " + b + " Selectivity " + s)
            plt.savefig(b + "_" + s + ".pdf")
            plt.cla()
            plt.clf()
