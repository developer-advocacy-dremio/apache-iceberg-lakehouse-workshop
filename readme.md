# Dremio Apache Iceberg Workshop

Welcome to the **Dremio Apache Iceberg Workshop**!

This hands-on workshop is designed to help you explore how the **Dremio Intelligent Lakehouse Platform** empowers modern Apache Iceberg lakehouse architectures. Throughout the workshop, you will gain practical experience with Dremio’s features and understand how they enhance performance, governance, and usability for Iceberg-based data environments.

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


## Reading the Data with Dremio



## Understanding Dremio's Autonomous Performance Management



## Creating Context with Dremio's Context/Semantic Layer



## Summary
