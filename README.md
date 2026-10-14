# E-Commerce Customer Segmentation with Apache Spark and Kafka

![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apache-spark&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?logo=apache-kafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)


A complete Big Data project that performs batch customer segmentation using RFM (Recency, Frequency, Monetary) analysis, compares clustering algorithms, and demonstrates real-time transaction enrichment using Apache Kafka and Spark Structured Streaming.

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Workflow](#2-workflow)
3. [Results and Analysis](#3-results-and-analysis)
4. [Dataset](#4-dataset)
5. [Prerequisites](#5-prerequisites)
6. [Getting Started](#6-getting-started)
7. [Execution Guide](#7-execution-guide)
8. [Repository Structure](#8-repository-structure)
9. [Notes and Limitations](#9-notes-and-limitations)
10. [Technologies](#10-technologies)
11. [Author](#11-author)

## 1. Project Overview

This repository implements an end-to-end data pipeline designed to segment the customers of an e-commerce website and apply these segments to continuously streaming data. The architecture is divided into two main phases:

- **Batch Processing**: uses Apache Spark and PySpark DataFrames to clean the data, engineer RFM features, and train clustering models with Spark MLlib.
- **Model Evaluation**: compares K-Means and Hierarchical Clustering (Ward linkage) algorithms using the silhouette score with squared Euclidean distance in order to select the optimal model.
- **Stream Processing**: uses Apache Kafka and Spark Structured Streaming to consume live JSON transaction events and enrich them with the precomputed customer segments.

**Environment**: fully containerized with Docker Compose for reproducible execution, with a Jupyter Notebook interface.

<div align="center">
  <img src="images/Architecture%20g%C3%A9n%C3%A9rale.jpeg" alt="General Architecture" width="90%"/>
  <br/>
  <em>Figure 1: General architecture of the e-commerce customer segmentation solution</em>
</div>

## 2. Workflow

1. **Data Ingestion**: load and explore the *Online Retail* dataset.
2. **Data Cleaning**: remove duplicate records, rows without a customer ID, cancelled invoices, and transactions with non-positive quantities or prices.
3. **Feature Engineering**: compute the revenue per transaction and aggregate RFM features at the customer level.
4. **Preprocessing**: apply logarithmic transformations and standardize the RFM features to ensure uniform scaling.
5. **Model Selection**: evaluate K-Means clustering for K values ranging from 2 to 8.
6. **Algorithm Comparison**: compare the optimal K-Means model with Hierarchical Clustering (Ward linkage) using two clusters and normalized feature vectors.
7. **Serialization**: select the algorithm with the highest silhouette score, analyze the resulting customer profiles, and save the final segmentation model output in Parquet format.
8. **Stream Enrichment**: consume JSON transactions from a Kafka topic and join them with the saved segmentation to assign cluster labels in real time.

> **Note**: the streaming pipeline uses the precomputed segmentation and does not retrain the model. Customers absent from the saved segmentation are assigned a null cluster (`null`).

## 3. Results and Analysis

The original *Online Retail* dataset contains 541,909 rows. After applying the preprocessing pipeline, the dataset is reduced to 392,692 valid transactions, representing 4,338 unique customers, 18,532 invoices, and 3,665 unique products.

### 3.1. K-Means Model Selection

The Spark MLlib K-Means implementation was evaluated for K values ranging from 2 to 8 using the `ClusteringEvaluator` with `distanceMeasure="squaredEuclidean"`.

| K | Silhouette Score | Training Cost |
|---|---|---|
| 2 | **0.6238** | 6486.51 |
| 3 | 0.5013 | 4878.00 |
| 4 | 0.4999 | 3961.85 |
| 5 | 0.4772 | 3330.25 |
| 6 | 0.4704 | 2874.82 |
| 7 | 0.4379 | 2595.93 |
| 8 | 0.4398 | 2361.80 |

Among the tested values, **K = 2** achieves the highest silhouette score. A score above 0.5 indicates a reasonable cluster structure and a relevant partitioning of the data.

### 3.2. Clustering Algorithm Comparison

K-Means and Hierarchical Clustering (Ward linkage) were compared using the same normalized RFM feature vectors and two clusters. The evaluation used the scikit-learn silhouette score with `metric="sqeuclidean"`.

| Algorithm | Clusters | Silhouette Score |
|---|---|---|
| **K-Means** | 2 | **0.6238** |
| Hierarchical Clustering (Ward) | 2 | 0.5567 |

K-Means achieved the highest silhouette score and was selected for the final saved segmentation. Both evaluation steps use squared Euclidean distance; the K-Means model selection score is computed by Spark MLlib, while the algorithm comparison scores are computed with scikit-learn.

### 3.3. Customer Profiles

The selected K-Means segmentation produces the following descriptive profiles:

| Cluster | Customers | Share | Avg. Recency (days) | Avg. Frequency (invoices) | Avg. Monetary Value |
|---|---|---|---|---|---|
| 0 | 1,654 | 38.13 % | 25.39 | 8.49 | 4,566.18 |
| 1 | 2,684 | 61.87 % | 133.15 | 1.68 | 497.30 |

- **Cluster 0**: high-value customers with recent purchases and high engagement.
- **Cluster 1**: lower-value customers with less frequent and less recent purchases.

These are descriptive profiles derived from the data, not predefined business labels.

## 4. Dataset

The project uses the *Online Retail* dataset.

- **Primary source**: [UCI Machine Learning Repository - Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)
- **Alternative mirror**: [erkansirin78/datasets - OnlineRetail.csv](https://github.com/erkansirin78/datasets)

The dataset is excluded from version control due to its size. Download it to `project/data/raw/OnlineRetail.csv` using the following command:

```bash
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv
```

## 5. Prerequisites

- **Docker** and **Docker Compose** installed and running (the Spark cluster, Kafka, and Jupyter are provided by the containers).
- At least **4 GB of RAM** allocated to Docker (8 GB recommended for running Spark and Kafka simultaneously).
- `curl` or a browser to download the dataset.
- No local installation of Spark, Kafka, or Java is required.

## 6. Getting Started

```bash
# 1. Clone the repository
git clone <REPOSITORY_URL>
cd <repository-name>

# 2. Download the dataset (see Section 4)
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv

# 3. Start the containerized environment
docker-compose up -d

# 4. Verify that the services are running
docker-compose ps
```

The Jupyter Notebook interface is accessible at: `http://localhost:8888`.

## 7. Execution Guide

The notebooks are located in the `project/notebooks/` folder.

1. **Batch phase**: open the segmentation notebook in Jupyter and execute the cells in order (cleaning → RFM features → preprocessing → K-Means (K = 2 to 8) → comparison with Ward → profile analysis → Parquet save).
2. **Streaming phase**: start the transaction producer (JSON events to the Kafka topic), then run the streaming notebook that consumes the stream and applies the join with the saved segmentation.
3. **Stopping the environment**:

```bash
docker-compose down
```

## 8. Repository Structure

```text
.
├── docker-compose.yml           # Service definitions (Spark, Kafka, Jupyter)
├── project/
│   └── notebooks/               # Batch and streaming notebooks
├── images/                      # Screenshots and README illustrations
│   └── Architecture générale.jpeg   # General architecture diagram (Figure 1)
├── .gitignore                   # Dataset and artifact exclusions
└── README.md                    # Project documentation
```

Notes:

- The `project/data/raw/` folder is not versioned: it is created at runtime by the download command (Section 4) and excluded from version control via `.gitignore`.
- The segmentation model output (Parquet format) is also generated at runtime and excluded from version control.

## 9. Notes and Limitations

- **Precomputed segmentation**: streaming does not train the model; it applies a join with the batch output. Unknown customers receive a `null` cluster.
- **Choice of K = 2**: statistically optimal according to the silhouette score, but a 3- or 4-group segmentation might offer more actionable granularity from a business perspective.
- **Mixed evaluation**: the silhouette scores come from two implementations (Spark MLlib and scikit-learn); although the metric is identical (`squaredEuclidean`), slight computational differences are possible.
- **RFM scope**: the segmentation relies solely on Recency, Frequency, and Monetary; integrating product features (basket, categories) would be a natural extension.
- **Historical dataset**: *Online Retail* covers the 2010–2011 period; the behavioral patterns do not reflect current e-commerce.

## 10. Technologies

| Technology | Usage |
|---|---|
| Apache Spark / PySpark | Cleaning, RFM feature engineering, batch clustering |
| Spark MLlib | K-Means, ClusteringEvaluator |
| scikit-learn | Comparative silhouette score (Ward vs K-Means) |
| Apache Kafka | Ingestion of JSON transaction events |
| Spark Structured Streaming | Real-time stream enrichment |
| Parquet | Serialization of the final segmentation |
| Docker Compose | Environment containerization and reproducibility |
| Jupyter Notebook | Interactive development and execution |

## 11. Author

<div align="center">

<table>
<tr>
<td align="center">
<a href="https://github.com/mohammed-boujaada">
<img src="https://github.com/mohammed-boujaada.png" width="120px;" alt="Mohammed Boujaada"/>
<br/>
<sub><b>Mohammed Boujaada</b></sub>
</a>
<br/>
MIATE Master's — FSA Nador
<br/>
<a href="https://github.com/mohammed-boujaada">
<img src="https://img.shields.io/badge/GitHub-@mohammed--boujaada-black?logo=github"/>
</a>
<a href="https://www.linkedin.com/in/mohammed-boujaada-229a3333a/">
<img src="https://img.shields.io/badge/LinkedIn-Boujaada%20Mohammed-blue?logo=linkedin"/>
</a>
</td>
</tr>
</table>

</div>

**Institution**: FSA Nador — Master's in Artificial Intelligence & Emerging Technologies  
**Module**: Big Data & Stream Processing  
**Academic Year**: 2025 - 2026

