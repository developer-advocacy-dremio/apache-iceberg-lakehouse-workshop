# Apache Iceberg Workshop

## Part 1 - Why Apache Iceberg and Dremio

![Workshop Slide 1](./img/slide1.png)

![Workshop Slide 2 - Meet Workshop Presenter](./img/slide2.png)

![Workshop Slide 3 - Download Iceberg and Polaris Books](./img/slide3.png)

![Workshop Slide 4 - The Status Quo](./img/slide4.png)

![Workshop Slide 5 - How Lakehouses Simplify Enterprise Data Platforms](./img/slide5.png)

![Workshop Slide 6 - The Dremio Lakehouse Platform](./img/slide6.png)

## Part 2 - Ingesting Data into Dremio Catalog

In this part of the workshop, we will use **Apache Spark** to write an Apache Iceberg table directly into the **Dremio Catalog** of our Dremio instance. This hands-on step is more than just a technical exercise—it demonstrates the **interoperability** at the heart of modern data lakehouse architecture.

The **Dremio Catalog** is powered by **Apache Polaris**, which implements the **Apache Iceberg REST Catalog interface**. This open standard enables you to connect your preferred engine or tool—such as Spark, Trino, Fivetran, Confluent, and many others—directly to the same catalog. By adhering to the REST Catalog specification, Dremio ensures that all connected tools and engines can operate on the same Iceberg tables without custom integrations or proprietary lock-in.

**Why this matters:**

- **Unified Data Management** – All your engines and tools point to the same source of truth for metadata and table definitions.
- **Cross-Engine Interoperability** – Move seamlessly between Spark, Trino, Fivetran, Confluent, and other compatible systems without data duplication.
- **Open Standards** – Built on the Apache Iceberg REST Catalog interface, ensuring flexibility and avoiding vendor lock-in.
- **Simplified Workflows** – Ingest, query, and transform data without moving it between disparate catalogs.

By the end of this section, you’ll see firsthand how simple it is to write data with Spark into the Dremio Catalog and immediately make that data available to any other compatible tool in your analytics stack.

## Part 3 – The Benefits of Dremio Catalog

Now that you’ve seen how easy it is to use **any tool** with the Dremio Catalog, it’s worth exploring the benefits of making it your **primary Apache Iceberg catalog**.

- **No Deployment Overhead** – The Dremio Catalog is fully integrated into your Dremio deployment, eliminating the need to manage a separate catalog service.
- **Identity Provider Integration** – Leverage Dremio’s built-in integrations with a variety of IdPs for seamless authentication and governance.
- **Granular Access Controls** – Apply **Role-Based Access Control (RBAC)** and **Fine-Grained Access Control (FGAC)** directly on your Iceberg tables to enforce data security and compliance.
- **Autonomous Table Optimization** – Let Dremio automatically manage the optimization of your Apache Iceberg tables. This removes the need to develop, orchestrate, and troubleshoot custom compaction or clustering pipelines, while ensuring your tables remain performant over time.

By relying on the Dremio Catalog, you get a secure, optimized, and fully managed Iceberg catalog experience that’s ready for interoperability across your entire data ecosystem.

