import os
import uuid
import time
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pytest

from lakefs import Client, Repository

TEST_STORAGE_NAMESPACE_BASE = os.getenv("STORAGE_NAMESPACE", "").rstrip("/")


def get_storage_namespace():
    return f"{TEST_STORAGE_NAMESPACE_BASE}/{uuid.uuid1()}"


def _setup_repo(namespace, default_branch):
    clt = Client()
    repo_name = "test-iceberg-catalog" + str(int(time.time()))
    repo = Repository(repo_name, clt)
    repo.create(storage_namespace=namespace, default_branch=default_branch)
    return clt, repo


def _get_data(spark):
    data_set = [(1, "James", "Smith", 32, "M"),
                (2, "Michael","Rose", 35, "M"),
                (3, "Robert", "Williams", 41, "M"),
                (4, "Maria", "Jones", 36, "F"),
                (5, "Jen","Brown", 44, "F"),
                (6, "Monika","Geller", 31, "F")]

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
    ])
    df = spark.createDataFrame(data=data_set, schema=schema)
    return df


@pytest.fixture(scope="session")
def setup_repo(spark, default_branch="main"):
    clt, repo = _setup_repo(get_storage_namespace(), default_branch)
    main = repo.branch("main")
    df = _get_data(spark)
    df.write.saveAsTable(f"lakefs.`{repo.id}`.main.company.workers")
    main.commit(message="Initial data load")
    return clt, repo


@pytest.fixture(scope="session")
def spark():
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    spark_config = SparkConf()
    spark_config.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog")
    spark_config.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.catalog.LakeFSCatalog")
    spark_config.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.catalog.LakeFSCatalog")
    spark_config.set("spark.sql.catalog.lakefs.cache-enabled", "false")
    spark_config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    spark_config.set("spark.hadoop.fs.s3a.access.key", access_key)
    spark_config.set("spark.hadoop.fs.s3a.secret.key", secret_key)
    spark_config.set("spark.hadoop.fs.lakefs.impl", "io.lakefs.LakeFSFileSystem")
    spark_config.set("spark.hadoop.fs.lakefs.access.key", "AKIAIOSFDNN7EXAMPLEQ")
    spark_config.set("spark.hadoop.fs.lakefs.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    spark_config.set("spark.hadoop.fs.lakefs.endpoint", "http://localhost:8000/api/v1")
    spark_config.set("spark.jars.packages",
                     "io.lakefs:lakefs-iceberg-catalog:0.1.0-SNAPSHOT,"
                     "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,"
                     "org.apache.hadoop:hadoop-aws:3.3.4"
                     )

    spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
    yield spark
    spark.stop()
