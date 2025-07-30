import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Fetch Dremio base URL and PAT from environment variables
DREMIO_BASE_URI = os.environ.get('DREMIO_BASE_URI')
DREMIO_PAT = os.environ.get('DREMIO_PAT')

if not DREMIO_BASE_URI or not DREMIO_PAT:
    raise ValueError("Please set environment variables DREMIO_BASE_URI and DREMIO_PAT.")

# Construct Dremio REST catalog and auth URIs
DREMIO_CATALOG_URI = f'http://{DREMIO_BASE_URI}:8181/api/catalog'
DREMIO_AUTH_URI = f'http://{DREMIO_BASE_URI}:9047/oauth/token'

# Configure Spark session with Iceberg and Dremio catalog settings
conf = (
    pyspark.SparkConf()
        .setAppName('DremioIcebergSparkApp')
        # Required external packages
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,com.dremio.iceberg.authmgr:authmgr-oauth2:0.0.5')
        # Enable Iceberg Spark extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        # Define Dremio catalog configuration using RESTCatalog
        .set('spark.sql.catalog.dremio', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.dremio.catalog-impl', 'org.apache.iceberg.rest.RESTCatalog')
        .set('spark.sql.catalog.dremio.uri', DREMIO_CATALOG_URI)
        .set('spark.sql.catalog.dremio.warehouse', 'default')  # Not used but required by Spark
        .set('spark.sql.catalog.dremio.cache-enabled', 'false')
        # Configure OAuth2 authentication using PAT
        .set('spark.sql.catalog.dremio.rest.auth.type', 'com.dremio.iceberg.authmgr.oauth2.OAuth2Manager')
        .set('spark.sql.catalog.dremio.rest.auth.oauth2.token-endpoint', DREMIO_AUTH_URI)
        .set('spark.sql.catalog.dremio.rest.auth.oauth2.grant-type', 'token_exchange')
        .set('spark.sql.catalog.dremio.rest.auth.oauth2.client-id', 'dremio')
        .set('spark.sql.catalog.dremio.rest.auth.oauth2.scope', 'dremio.all')
        .set('spark.sql.catalog.dremio.rest.auth.oauth2.token-exchange.subject-token', DREMIO_PAT)
        .set('spark.sql.catalog.dremio.rest.auth.oauth2.token-exchange.subject-token-type', 'urn:ietf:params:oauth:token-type:dremio:personal-access-token')
)

# Initialize Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("✅ Spark session connected to Dremio Catalog.")

# Step 1: Create a namespace (schema) in the Dremio catalog
spark.sql("CREATE NAMESPACE IF NOT EXISTS dremio.demo")

# Step 2: Create sample Iceberg tables in the Dremio catalog
spark.sql("""
CREATE TABLE IF NOT EXISTS dremio.demo.customers (
    id INT,
    name STRING,
    email STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS dremio.demo.orders (
    order_id INT,
    customer_id INT,
    amount DOUBLE
)
USING iceberg
""")

# Step 3: Insert sample data into the tables
customers_data = [
    Row(id=1, name="Alice", email="alice@example.com"),
    Row(id=2, name="Bob", email="bob@example.com")
]

orders_data = [
    Row(order_id=101, customer_id=1, amount=250.50),
    Row(order_id=102, customer_id=2, amount=99.99)
]

customers_df = spark.createDataFrame(customers_data)
orders_df = spark.createDataFrame(orders_data)

customers_df.writeTo("dremio.demo.customers").append()
orders_df.writeTo("dremio.demo.orders").append()

print("✅ Tables created and sample data inserted.")
