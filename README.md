# E-commerce Customer Segmentation with Apache Spark and Kafka

A comprehensive Big Data project that performs batch customer segmentation using RFM (Recency, Frequency, Monetary) analysis, compares clustering algorithms, and demonstrates real-time transaction enrichment using Apache Kafka and Spark Structured Streaming.

## Table of Contents

- [Project Overview](#project-overview)
- [Workflow](#workflow)
- [Results and Analysis](#results-and-analysis)
- [Dataset](#dataset)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Execution Guide](#execution-guide)
- [Repository Structure](#repository-structure)
- [Notes and Limitations](#notes-and-limitations)
- [Technologies](#technologies)

## Project Overview

This repository implements an end-to-end data pipeline designed to segment e-commerce customers and apply these segments to streaming data. The architecture is divided into two main phases:

- **Batch Processing:** Utilizes Apache Spark and PySpark DataFrames to clean data, engineer RFM features, and train clustering models using Spark MLlib.
- **Model Evaluation:** Compares K-Means and Agglomerative Clustering (Ward linkage) using the silhouette score with squared Euclidean distance to select the optimal model.
- **Stream Processing:** Utilizes Apache Kafka and Spark Structured Streaming to consume live JSON transaction events and enrich them with the pre-computed customer segments.
- **Environment:** Fully containerized using Docker Compose for reproducible execution, featuring a Jupyter Notebook interface.

## Workflow

1. **Data Ingestion:** Load and explore the Online Retail dataset.
2. **Data Cleaning:** Remove duplicate records, rows missing customer IDs, cancellation invoices, and transactions with non-positive quantities or prices.
3. **Feature Engineering:** Calculate transaction revenue and aggregate customer-level RFM features.
4. **Preprocessing:** Apply log transformations and standardize the RFM features to ensure uniform scaling.
5. **Model Selection:** Evaluate K-Means clustering for K values ranging from 2 to 8.
6. **Algorithm Comparison:** Compare the optimal K-Means model against Agglomerative Clustering (Ward linkage) using two clusters and standardized feature vectors.
7. **Serialization:** Select the algorithm with the highest silhouette score, analyze the resulting customer profiles, and save the final segmentation model output as a Parquet file.
8. **Stream Enrichment:** Consume JSON transactions from a Kafka topic and join them with the saved segmentation to assign real-time cluster labels.

*Note: The streaming pipeline utilizes the pre-computed segmentation and does not retrain the model. Customers absent from the saved segmentation will be assigned a null cluster.*

## Results and Analysis

The original Online Retail dataset contains 541,909 rows. After applying the preprocessing pipeline, the dataset is reduced to approximately 392,692 valid transactions, representing 4,338 unique customers, 18,532 invoices, and 3,665 unique products.

### K-Means Model Selection

The Spark MLlib K-Means implementation was evaluated for K values from 2 through 8 using the `ClusteringEvaluator` with `distanceMeasure="squaredEuclidean"`.

| K | Silhouette Score | Training Cost |
|---|------------------|---------------|
| 2 | 0.6238           | 6486.51       |
| 3 | 0.5013           | 4878.00       |
| 4 | 0.4999           | 3961.85       |
| 5 | 0.4772           | 3330.25       |
| 6 | 0.4704           | 2874.82       |
| 7 | 0.4379           | 2595.93       |
| 8 | 0.4398           | 2361.80       |

Among the tested values, **K = 2** yields the highest silhouette score.

### Clustering Algorithm Comparison

K-Means and Agglomerative Clustering (Ward linkage) were compared using the same standardized RFM feature vectors and two clusters. The evaluation utilized scikit-learn's silhouette score with `metric="sqeuclidean"`.

| Algorithm                      | Clusters | Silhouette Score |
|--------------------------------|----------|------------------|
| K-Means                        | 2        | 0.6238           |
| Agglomerative Clustering (Ward)| 2        | 0.5567           |

K-Means achieved the higher silhouette score and was selected for the final saved segmentation. Both evaluation steps utilize squared Euclidean distance; the K-Means model-selection score is calculated by Spark MLlib, while the algorithm-comparison scores are calculated with scikit-learn.

### Customer Profiles

The selected K-Means segmentation produces the following descriptive profiles:

| Cluster | Customers | Share  | Avg. Recency (days) | Avg. Frequency (invoices) | Avg. Monetary Value |
|---------|-----------|--------|---------------------|---------------------------|---------------------|
| 0       | 1,654     | 38.13% | 25.39               | 8.49                      | 4,566.18            |
| 1       | 2,684     | 61.87% | 133.15              | 1.68                      | 497.30              |

Cluster 0 represents high-value customers with recent purchases and high engagement. Cluster 1 represents lower-value customers with infrequent and less recent purchases. These are descriptive profiles inferred from the data, not predefined business labels.

## Dataset

The project utilizes the **Online Retail** dataset.

- **Primary Source:** [UCI Machine Learning Repository - Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail)
- **Alternative Mirror:** [erkansirin78/datasets - OnlineRetail.csv](https://github.com/erkansirin78/datasets/blob/master/OnlineRetail.csv)

The dataset is excluded from version control due to its size. Download it to `project/data/raw/OnlineRetail.csv` using the following command:

```bash
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv