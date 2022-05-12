from argparse import ArgumentParser

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

def bar_counts():
    parser = ArgumentParser()
    parser.add_argument("csvfile")
    parser.add_argument("outfile")
    args = parser.parse_args()

    df = pd.read_csv(args.csvfile)
    df.sort_values("count", axis=0, ascending=False, inplace=True)
    print(df.columns)
    df.rename(columns={"count": "Count"}, inplace=True)
    print(df)


    sns.set(rc={'figure.figsize': (6, 10)})
    sns.set(font_scale=1.2)
    sns.set_style("whitegrid")
    plt.figure()
    sns.barplot(data=df.reset_index(), x="Count", y="Language", color="#003478")
    plt.suptitle("Article Counts by Language")
    plt.xticks(
        rotation=30,
        horizontalalignment="center"
    )
    plt.tight_layout()
    plt.autoscale()
    plt.savefig("document_count.png", dpi=200)
    plt.show()

if __name__ == "__main__":
    bar_counts()