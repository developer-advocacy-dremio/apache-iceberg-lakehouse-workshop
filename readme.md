# Dremio Apache Iceberg Workshop

Welcome to the **Dremio Apache Iceberg Workshop**!

This hands-on workshop is designed to help you explore how the **Dremio Intelligent Lakehouse Platform** empowers modern Apache Iceberg lakehouse architectures. Throughout the workshop, you will gain practical experience with Dremio’s features and understand how they enhance performance, governance, and usability for Iceberg-based data environments.

## Purpose of Workshop

Modern data teams often work in silos, each using different tools and platforms to meet their specific needs. This fragmentation can make it difficult to share data, enforce consistency, and deliver timely insights across the organization. Apache Iceberg addresses this challenge by providing an open, interoperable table format that supports reliable, high-performance analytics across a variety of engines and tools.

However, while Iceberg brings powerful capabilities, running a production-grade Iceberg-based data lakehouse introduces new complexities—such as setting up and maintaining a metadata catalog, optimizing tables for performance, governing access, and accelerating queries. These operational concerns can become a barrier to adoption.

This workshop introduces Dremio as a platform purpose-built to simplify and scale Apache Iceberg lakehouses. With an integrated catalog powered by Apache Polaris, Dremio removes the friction of catalog deployment and governance. Its autonomous performance management features—including intelligent caching, query acceleration, and table optimization—ensure that your Iceberg tables deliver fast and consistent performance. Combined with Dremio’s semantic layer for business context and collaboration, the platform turns a complex lakehouse architecture into a manageable, user-friendly environment.

Through hands-on exercises, this workshop will guide you in connecting to a Dremio-backed Iceberg catalog, creating tables, inserting data, and observing how Dremio makes managing and using an Iceberg lakehouse both accessible and efficient.


## Table of Contents

- [What You’ll Learn](#what-youll-learn)
- [Requirements](#-requirements)
- [No Enterprise Access? Try the Self-Guided Tutorial](#no-enterprise-access-try-the-self-guided-tutorial)
- [Setup](#setup)
- [Ingesting Data with Spark](#ingesting-data-with-spark)
- [Reading the Data with Dremio](#reading-the-data-with-dremio)
- [Understanding Dremio's Autonomous Performance Management](#understanding-dremios-autonomous-performance-management)
- [Creating Context with Dremio's Context/Semantic Layer](#creating-context-with-dremios-contextsemantic-layer)
- [Summary](#summary)


## What You’ll Learn

In this workshop, we will walk through:

- How to connect **Apache Iceberg catalogs** to Dremio, including external catalogs like Nessie, Glue, and Polaris
- The capabilities and benefits of **Dremio’s integrated Iceberg catalog**
- How Dremio delivers **end-to-end performance management** for Apache Iceberg tables through features like reflections, caching, and table optimization
- An overview of additional **Dremio features**, including:
  - The **Semantic Layer** for defining and curating business-friendly datasets
  - **Semantic Search** for discovering data using natural language
  - **Wikis** and other collaboration tools that make data documentation seamless and accessible

## 🛠 Requirements

To complete the exercises in this workshop, you will need the following:

- [Docker](https://www.docker.com/) installed and running on your machine
- Access to a **Dremio Enterprise Edition** environment (either self-hosted or cloud-based)

> **Note:** Some features covered in this workshop—such as the integrated catalog, autonomous performance management, and semantic search—are exclusive to Dremio Enterprise Edition.

## No Enterprise Access? Try the Self-Guided Tutorial

If you don’t currently have access to Dremio Enterprise Edition, you can still gain valuable hands-on experience by following a self-guided tutorial using Docker and **Dremio Community Edition**.

This tutorial covers:

- How to connect external Apache Iceberg catalogs to Dremio
- How to explore and query Iceberg tables
- How to use Dremio’s Semantic Layer with virtual datasets

👉 [Start the Self-Guided Tutorial](https://drmevn.fyi/lakehouse-on-laptop-ce)

Feel free to clone this repository and follow along. If you have any questions or feedback, don’t hesitate to open an issue or reach out!

## Setup

To begin working with Apache Iceberg tables using Spark and Dremio, we need to configure a few environment variables and launch our Spark containerized environment. This setup ensures secure and seamless connectivity between Spark and Dremio's REST catalog for table operations.

[Docker Compose File Content](https://github.com/developer-advocacy-dremio/apache-iceberg-lakehouse-workshop/blob/main/docker-compose.yaml)

If you didn't clone this repo then make sure to copy the contents of this file into a file called `docker-compose.yml`

### 1. Define Environment Variables in the Host

Before launching the container, define the following environment variables in your terminal session. These values will be passed into the Spark container to enable secure communication with your Dremio instance.

```bash
export DREMIO_BASE_URI=localhost      # The base host name or IP where Dremio is running
export DREMIO_PAT=your_dremio_pat     # A Personal Access Token used for authentication
```

**Why this matters:**
Spark needs to authenticate with Dremio’s REST Catalog in order to create, query, and manage Apache Iceberg tables. By injecting these values into the container as environment variables, we avoid hardcoding sensitive information in the Docker Compose file and ensure flexibility across environments.

**Note:** If Dremio is running on a different host or in a cloud environment, adjust DREMIO_BASE_URI accordingly.

### 2. Launch the Spark Notebook Container
Once your environment variables are defined, start your environment using:

```bash
docker-compose up spark
```

**Why this matters:**
This will launch a JupyterLab notebook environment preconfigured with Spark, Iceberg, and the necessary Hadoop and AWS libraries. The container is set up to:

- Connect to Dremio as an Iceberg catalog

- Provide a local workspace for development

- Run Spark in standalone mode with a master, worker, and history server

- Expose a browser-based JupyterLab interface for writing and executing PySpark code

After the container starts, you can access the notebook interface by visiting:

```
http://localhost:8888
```

No password is required, and you’ll find your seeded notebooks and scripts under the /workspace/seed-data directory.

This interactive setup gives you full control over creating namespaces, managing Iceberg tables, and testing queries against your Dremio lakehouse—all without leaving your browser.


## Ingesting Data with Spark

Now that your environment is up and running, it's time to ingest data into Apache Iceberg tables using Spark. In this step, we'll use a PySpark script to create an Iceberg namespace, define a couple of tables, and insert sample records—all within the Dremio catalog.

The complete script used in this section can be found here:  
[https://github.com/developer-advocacy-dremio/apache-iceberg-lakehouse-workshop/blob/main/spark.py](https://github.com/developer-advocacy-dremio/apache-iceberg-lakehouse-workshop/blob/main/spark.py)

We'll walk through it piece by piece:

### 1. Load Required Credentials

```python
DREMIO_BASE_URI = os.environ.get('DREMIO_BASE_URI')
DREMIO_PAT = os.environ.get('DREMIO_PAT')
```

**What's happening:**
Before Spark can connect to Dremio’s REST-based Iceberg catalog, it needs the base URI and a personal access token (PAT) for authentication. These are fetched securely from environment variables that you set during the setup step.

**Why this matters:**
This pattern helps keep credentials out of source code, making your workflow more secure and portable across different environments.

### 2. Define REST Endpoints for Dremio Catalog and Authentication
```python
DREMIO_CATALOG_URI = f'http://{DREMIO_BASE_URI}:8181/api/catalog'
DREMIO_AUTH_URI = f'http://{DREMIO_BASE_URI}:9047/oauth/token'
```

**What's happening:**
These variables define where Spark should send catalog operations and where to authenticate the session using OAuth2. These endpoints follow Dremio’s REST API specifications.

**Why this matters:**
Proper endpoint configuration ensures Spark knows where and how to securely interact with Dremio’s integrated Apache Polaris catalog.

### 3. Configure the Spark Session for Iceberg and Dremio
```python
conf = (
    pyspark.SparkConf()
        ...
        .set('spark.sql.catalog.dremio', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.dremio.catalog-impl', 'org.apache.iceberg.rest.RESTCatalog')
        .set('spark.sql.catalog.dremio.uri', DREMIO_CATALOG_URI)
        ...
        .set('spark.sql.catalog.dremio.rest.auth.oauth2.token-endpoint', DREMIO_AUTH_URI)
        ...
        .set('spark.sql.catalog.dremio.rest.auth.oauth2.token-exchange.subject-token', DREMIO_PAT)
)
```

**What's happening:**
This section creates a Spark configuration object that enables Iceberg support and connects it to a custom catalog named dremio using the RESTCatalog implementation. It includes all the necessary OAuth2-based authentication details.

**Why this matters:**
Spark needs this context to execute Iceberg operations through Dremio, and this configuration provides all the pieces needed to do so—plugin, authentication, and endpoint metadata.

### 4. Start the Spark Session
```python
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

**What's happening:**
This line starts a new Spark session using the custom configuration created earlier. At this point, Spark is ready to talk to the Dremio Iceberg catalog.

**Why this matters:**
All subsequent SQL or DataFrame commands will be executed against Iceberg tables in Dremio’s namespace.

### 5. Create a Namespace (Schema)
```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS dremio.demo")
```

**What's happening:**
This creates a logical grouping (like a schema or database) named demo under the dremio catalog if it doesn’t already exist.

**Why this matters:**
Namespaces help organize tables and provide a foundation for managing access, lineage, and structure in Iceberg.

### 6. Create Iceberg Tables
```python
spark.sql("""
CREATE TABLE IF NOT EXISTS dremio.demo.customers (
    id INT,
    name STRING,
    email STRING
)
USING iceberg
""")
```

```python
spark.sql("""
CREATE TABLE IF NOT EXISTS dremio.demo.orders (
    order_id INT,
    customer_id INT,
    amount DOUBLE
)
USING iceberg
""")
```

**What's happening:**
These commands create two tables: customers and orders inside the dremio.demo namespace. Each table has a simple schema and is declared explicitly as USING iceberg.

**Why this matters:**
You are now defining Iceberg tables directly from Spark into the Dremio catalog, enabling interoperability across tools and engines.

### 7. Insert Sample Data Using DataFrames
```python
customers_data = [
    Row(id=1, name="Alice", email="alice@example.com"),
    Row(id=2, name="Bob", email="bob@example.com")
]

orders_data = [
    Row(order_id=101, customer_id=1, amount=250.50),
    Row(order_id=102, customer_id=2, amount=99.99)
]
```
```python
customers_df = spark.createDataFrame(customers_data)
orders_df = spark.createDataFrame(orders_data)
```

**What's happening:**
We're defining sample customer and order data as lists of Row objects, then converting them into Spark DataFrames.

**Why this matters:**
This step gives us small, clean datasets for testing ingestion and querying functionality.

### 8. Write the Data to Iceberg Tables
```python
customers_df.writeTo("dremio.demo.customers").append()
orders_df.writeTo("dremio.demo.orders").append()
```

**What's happening:**
The `.writeTo(...).append()` method pushes the sample data into the respective Iceberg tables.

**Why this matters:**
This demonstrates writing structured data into Iceberg format using Spark, and these writes are fully managed and tracked via Dremio’s integrated catalog and metadata engine.

### Final Output
```python
print("✅ Tables created and sample data inserted.")
```

This confirms the ingestion pipeline was successful. You can now query the customers and orders tables using Dremio or any engine connected to the catalog.

### Next Steps
With your data ingested into Iceberg tables, the next section will explore how to visualize and query this data through the Dremio UI and examine its metadata using Iceberg system tables

## Reading the Data with Dremio



## Understanding Dremio's Autonomous Performance Management



## Creating Context with Dremio's Context/Semantic Layer



## Summary
