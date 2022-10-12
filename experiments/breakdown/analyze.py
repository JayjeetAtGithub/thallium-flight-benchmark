import math
from unittest import result
import matplotlib.pyplot as plt
import seaborn


def round_up(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier


if __name__ == "__main__":

    result_dict = dict()

    labels = [1, 10, 100]

    for l in labels:
        print("Selectivity: ", l, " %")
        with open(f"client_{l}", "r") as cf:
            lines = cf.readlines()
            for line in lines:
                line_split = line.split(": ")
                key = line_split[0]
                if result_dict.get(key, None) != None:
                    try:
                        result_dict[key].append(float(line_split[1].split(" ")[0]))
                    except:
                        pass
                else:
                    result_dict[key] = list()

        with open(f"server_{l}", "r") as cf:
            lines = cf.readlines()
            for line in lines:
                line_split = line.split(": ")
                key = line_split[0]
                if result_dict.get(key, None) != None:
                    try:
                        result_dict[key].append(float(line_split[1].split(" ")[0]))
                    except:
                        pass
                else:
                    result_dict[key] = list()

        # print(result_dict)
        result_dict.pop("Using bake backend")
        # print(result_dict.keys())

        for key in result_dict.keys():
            result_dict[key] = sum(result_dict[key])

        # print(result_dict)

        total = 0
        for key in result_dict.keys():
            if key != "scan_file":
                total += result_dict[key]
        
        # print(total)
        percentages = list()
        stage_labels = list()
        for key in result_dict.keys():
            if key != "scan_file":
                print(key, " :", round_up(float(result_dict[key]/total), 5) * 100, "%")
                percentages.append(round_up(float(result_dict[key]/total), 5) * 100)
                stage_labels.append(key)
        
        data = percentages
        keys = stage_labels

        print(data)
        print(keys)
        
        palette_color = seaborn.color_palette('bright')
        plt.pie(data, labels=keys, colors=palette_color, autopct='%.2f%%')
        plt.savefig(f"plot_{l}.pdf")
        plt.cla()
        plt.clf()

        print("------------------------------------------------")
        result_dict = dict()
