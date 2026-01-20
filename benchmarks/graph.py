import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def generate_full_report(window_size=30):
    print("Loading data and generating visualization suite...")

    try:
        base = pd.read_csv("baseline.csv")
        opt = pd.read_csv("optimized.csv")
    except FileNotFoundError:
        print("Error: Ensure baseline.csv and optimized.csv exist in this directory.")
        return

    # Pre-processing
    base_reads = base[base["type"] == "READ"].sort_values("elapsed_sec")
    opt_reads = opt[opt["type"] == "READ"].sort_values("elapsed_sec")

    # GRAPH: ROLLING WINDOW OVERLAY (Consistency & Optimization)
    plt.figure(figsize=(12, 6))
    plt.plot(
        base_reads["elapsed_sec"],
        base_reads["latency_ms"],
        color="red",
        alpha=0.1,
        label="Baseline Raw",
    )
    plt.plot(
        base_reads["elapsed_sec"],
        base_reads["latency_ms"].rolling(window_size).mean(),
        color="red",
        linewidth=2,
        label="Baseline Trend",
    )
    plt.plot(
        opt_reads["elapsed_sec"],
        opt_reads["latency_ms"],
        color="green",
        alpha=0.1,
        label="Optimized Raw",
    )
    plt.plot(
        opt_reads["elapsed_sec"],
        opt_reads["latency_ms"].rolling(window_size).mean(),
        color="green",
        linewidth=2,
        label="Optimized Trend",
    )
    plt.title("Query Latency: Rolling Average Optimization Trend")
    plt.ylabel("Latency (ms)")
    plt.xlabel("Seconds Elapsed")
    plt.legend()
    plt.savefig("trend_overlay.png")


    # GRAPH: CUMULATIVE THROUGHPUT
    plt.figure(figsize=(12, 6))
    plt.plot(
        base_reads["elapsed_sec"],
        np.arange(len(base_reads)),
        color="red",
        label="Baseline (Queries Completed)",
    )
    plt.plot(
        opt_reads["elapsed_sec"],
        np.arange(len(opt_reads)),
        color="green",
        label="Optimized (Queries Completed)",
    )
    plt.title("Cumulative Throughput: Total Work Done Over Time")
    plt.ylabel("Total Queries Processed")
    plt.xlabel("Seconds Elapsed")
    plt.legend()
    plt.savefig("throughput_line.png")

    # GRAPH: READ/WRITE TRADE-OFF
    plt.figure(figsize=(10, 6))
    # Calculate means
    tradeoff_data = {
        "Metric": ["Read Latency", "Read Latency", "Write Latency", "Write Latency"],
        "System": ["Baseline", "Optimized", "Baseline", "Optimized"],
        "ms": [
            base[base["type"] == "READ"]["latency_ms"].mean(),
            opt[opt["type"] == "READ"]["latency_ms"].mean(),
            base[base["type"] == "WRITE"]["latency_ms"].mean(),
            opt[opt["type"] == "WRITE"]["latency_ms"].mean(),
        ],
    }
    df_tradeoff = pd.DataFrame(tradeoff_data)
    sns.barplot(
        x="Metric",
        y="ms",
        hue="System",
        data=df_tradeoff,
        palette={"Baseline": "red", "Optimized": "green"},
    )
    plt.title("The Indexing Trade-off: Read Benefit vs. Write Tax")
    plt.savefig("tradeoff_bar.png")

    print("Report Generation Complete. Check your folder for 4 PNG files!")


if __name__ == "__main__":
    generate_full_report()
