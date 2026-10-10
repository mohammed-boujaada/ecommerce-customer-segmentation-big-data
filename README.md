# E-commerce Customer Segmentation with Apache Spark and Kafka

A Big Data project combining batch customer segmentation with a Kafka-based streaming demonstration. The batch pipeline builds RFM (Recency, Frequency, Monetary) features and applies K-Means clustering with Apache Spark MLlib. A second notebook consumes transaction events from Kafka and associates recognized customers with their previously computed segment.

## Project overview

- **Data processing:** PySpark DataFrames and educational RDD operations.
- **Customer analytics:** RFM feature engineering.
- **Machine learning:** log transformation, feature assembly, standardization, and K-Means clustering.
- **Model evaluation:** silhouette scores for K values from 2 through 8.
- **Streaming:** Kafka + Spark Structured Streaming.
- **Environment:** Docker Compose, Jupyter, PySpark, and Apache Kafka.

## Architecture

1. Load and explore the Online Retail dataset.
2. Remove duplicate rows, records without a customer ID, cancelled invoices, and rows with non-positive quantities or prices.
3. Calculate transaction revenue and aggregate customer-level RFM features.
4. Log-transform and standardize RFM features.
5. Evaluate K-Means models for K = 2 to 8 and select K = 2 based on the highest silhouette score among the tested values.
6. Save each customer's RFM features and cluster assignment as Parquet.
7. Consume JSON transaction events from Kafka, calculate revenue, and left-join recognized customers to the saved segmentation.

The streaming notebook **does not retrain** the clustering model. Customers not present in the saved segmentation receive a null cluster. The streaming query uses a memory sink for demonstration.

## Results

The notebook processed the Online Retail dataset as follows:

| Metric | Result |
|---|---:|
| Original transactions | 541,909 |
| Transactions after cleaning | 392,692 |
| Distinct customers | 4,338 |
| Distinct invoices | 18,532 |
| Distinct products | 3,665 |
| Total revenue in the dataset's currency units | 8,887,208.89 |
| Best tested K | 2 |
| Silhouette score at K = 2 | 0.6238 |

### Cluster profiles

| Cluster | Customers | Share | Average recency (days) | Average frequency (invoices) | Average monetary value |
|---|---:|---:|---:|---:|---:|
| 0 | 1,654 | 38.13% | 25.39 | 8.49 | 4,566.18 |
| 1 | 2,684 | 61.87% | 133.15 | 1.68 | 497.30 |

Cluster 0 contains customers with more recent purchases and higher average frequency and monetary value. Cluster 1 contains customers with less recent purchases and lower average frequency and monetary value. These are descriptive profiles inferred from the clustering results, not predefined customer labels.

The silhouette score is the best among the tested values K = 2–8; it does not establish that two clusters are universally optimal for every use case.

## Dataset

The project uses the **Online Retail** dataset.

- **Primary source:** [UCI Machine Learning Repository — Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail)
- **Alternative download mirror:** [erkansirin78/datasets — OnlineRetail.csv](https://github.com/erkansirin78/datasets/blob/master/OnlineRetail.csv)

Download the CSV to `project/data/raw/OnlineRetail.csv`. The dataset is not included in this repository.

From the repository root, run:

```bash
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv
```

Check the UCI page for dataset attribution and licensing information before redistributing the data.

## Requirements

- Docker Engine
- Docker Compose
- A web browser

## Getting started

Clone the repository:

```bash
git clone https://github.com/moza369/ecommerce-customer-segmentation-big-data.git
cd ecommerce-customer-segmentation-big-data
```

Download the dataset using the command above, then start the services:

```bash
docker compose up -d
docker compose logs jupyter
```

Open Jupyter at [http://localhost:8888](http://localhost:8888). If authentication is requested, use the token printed in the Jupyter container logs.

The Compose configuration exposes:
- Jupyter: `localhost:8888`
- Spark UI: `localhost:4040` while a Spark application is running
- Kafka: `localhost:9092` from the host; use `kafka:9092` between containers on the Compose network

## Run the notebooks

Run the notebooks in this order.

### 1. Batch customer segmentation

Open `project/notebooks/01_customer_segmentation_clean.ipynb` and run its cells in order.

It cleans the data, constructs RFM features, evaluates K-Means models, profiles the clusters, and writes the customer segmentation to:

`/home/jovyan/work/data/processed/customer_segments`

This generated Parquet output is required by the streaming notebook.

### 2. Start the Kafka streaming notebook

Open `project/notebooks/02_kafka_streaming_clean.ipynb` and run its cells in order. The notebook subscribes to the `customer-transactions` topic using `kafka:9092`.

**Important:** the stream uses `startingOffsets=latest`. Start the streaming query before publishing test messages, or messages published earlier may not be consumed by that query.

### 3. Create the Kafka topic

In a terminal, from the repository root, run:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic customer-transactions \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 4. Publish a test transaction

The streaming notebook expects JSON with these fields and types:

- `CustomerID`: integer
- `InvoiceNo`: string
- `Quantity`: integer
- `UnitPrice`: number

After the streaming query is active, run this from the repository root to publish one example message:

```bash
printf '%s\n' '{"CustomerID":17850,"InvoiceNo":"DEMO-1001","Quantity":2,"UnitPrice":5.50}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 \
      --topic customer-transactions
```

`17850` appears in the cleaned dataset shown in the batch notebook output and is used here as an example customer. If you want to test a different customer, choose a `CustomerID` present in the saved segmentation. A customer not found in that Parquet output will have a null `Cluster`.

The notebook displays the processed stream by querying its in-memory table, `live_customer_segments`. The memory sink is for demonstration and is not a durable output store.

## Repository structure

```text
.
├── .gitignore
├── README.md
├── docker-compose.yml
└── project/
    └── notebooks/
        ├── 01_customer_segmentation_clean.ipynb
        └── 02_kafka_streaming_clean.ipynb
```

The raw dataset, generated Parquet files, and streaming checkpoints are excluded from version control.

## Notes and limitations

- The streaming pipeline enriches transactions using a previously generated segmentation; it does not retrain the model.
- The stream expects the batch notebook's Parquet output to exist before the streaming notebook starts.
- Kafka events must be published after the streaming query starts because the notebook uses `startingOffsets=latest`.
- The streaming notebook uses a memory sink and a local checkpoint directory for demonstration.
- The Compose file uses container image tags and package versions as currently specified in the project. A clean-machine setup has not yet been independently verified.

## Technologies

Python · Apache Spark · PySpark · Spark SQL · Spark MLlib · Apache Kafka · Structured Streaming · K-Means · RFM Analysis · Docker Compose · Jupyter
