# Final project for BDT course

## Project Overview

1. End-to-End Big Data Pipeline
   - Project involves building a full pipeline integrating real-time data ingestion,
     processing, storage, and visualization.

2. Real-Time Streaming Data
   - Students work with high-volume, real-time streaming data from public sources to
     simulate practical scenarios.

3. Distributed Computing and Storage
   - Processing uses distributed computing and stores results in NoSQL or distributed
     data warehouses for scalability.

4. Visualization and Insights
   - Results are presented via live dashboards that update frequently to show analytics
     insights clearly.

## Architecture Overview

- Data Ingestion with Kafka
  - Data is ingested from real-time sources into Kafka, providing scalable,
    durable buffering for downstream processing.
- Distributed Processing with Spark
  - Spark Structured Streaming performs distributed filtering, aggregation, joins,
    and anomaly detection on the ingested data.
- Persistent Storage Layer
  - Processed data is stored in persistent layers like HBase or Hive to support
    various access and query patterns.
- Visualization and Insight Delivery
  - Dashboards connect to storage layers, presenting real-time or near real-time
    insights to end users.

See [screenshot.png](screenshot.png) for project details and points for each part.

## Project Parts Details

### Part 1. [3] Real-Time Data Ingestion (Apache Kafka)

- Identify a free, high-volume, real-time data source (e.g., Binance WebSocket for
  cryptocurrency trades, Wikimedia Recent Changes stream, Meetup.com RSVP
  stream, or a public IoT sensor feed).

- Write a producer script (Java or Python) that connects to this API and streams
  the raw data into an Apache Kafka topic.

### Part 2. [3] Distributed Processing (Spark Structured Streaming)

- Write an Apache Spark Structured Streaming application that subscribes to your
  Kafka topic.

- Your Spark application must perform meaningful transformations or
  aggregations on the data in real time (e.g., windowed aggregations, filtering
  anomalies, joining with static reference data, or calculating moving averages).

### Part 3. [2] Persistent Storage (HBase or Hive)

- Sink your processed DataFrames from Spark into a persistent storage layer.
- Option A (HBase): Best for storing aggregated data that requires rapid, real-time
  key-value lookups for your dashboard.
- Option B (Hive): Best for appending structured, batch-oriented summaries of
  your streaming windows.

### Part 4. [2] Visualization & Dashboarding

- Connect your persistent storage layer to a visualization tool to create a live (or
  frequently updating) dashboard of your insights.
- You may use the ELK Stack (Elasticsearch/Kibana), Streamlit, Grafana, Tableau, or
  a custom web application.

### Part 5. [2 bonus points] Data Enrichment with Spark SQL

- To earn bonus points on your final project grade, use Spark SQL to join your live
  streaming data with a static dataset stored in HDFS.
- Example: If your stream contains raw transaction IDs, load a static CSV of product
  details from HDFS and join them together in your Spark code to enrich the data
  before writing it to your database.

See [sample_flow.png](sample_flow.png) for a visual representation of the project's data pipeline flow.

## Static Public Datasets

If you are unable to locate real-time Streaming data sources,
then look at the following sites and find your favorite static
dataset to simulate streaming and do further analysis.

- [Amazon Web Services](https://aws.amazon.com/public-datasets/)
- [UCI Machine Learning Repository](https://archive.ics.uci.edu/)
- [Kaggle](https://www.kaggle.com/datasets)
- [US Government's Open Data](https://data.gov/)

---

# Project Deliverables

## 1. Source Code Repository Submission

- Provide a link to a public GitHub repository containing all your project files,
  including your Kafka producer scripts, Spark application code, database
  schema definitions, and visualization configurations.
- Include a README.md file with clear instructions on how to start your
  pipeline.

---

## 2. Video Demonstration (OBS Studio , Loom or Streams could be used for recording)

- Record a video presentation (max 20 mins) showcasing your project.
- Upload the video to Microsoft Streams and submit the link.
- Show the data flowing from the source API into Kafka, through Spark, and
  finally updating your visualization dashboard in real time.
- All team members are required to appear in the video. Each member must
  be visibly present and actively participate in the demo presentation to get
  points.
