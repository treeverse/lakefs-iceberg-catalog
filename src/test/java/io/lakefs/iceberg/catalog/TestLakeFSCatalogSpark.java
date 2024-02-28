package io.lakefs.iceberg.catalog;

import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.model.RepositoryCreation;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import io.lakefs.LakeFSClient;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

public class TestLakeFSCatalogSpark {
    @Test
    public void testLakeFSWithSpark() throws AnalysisException, IOException, ApiException {
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String storageNamespace = System.getenv("STORAGE_NAMESPACE");
        
        Configuration lakefsConf = new Configuration();
        lakefsConf.set("fs.lakefs.access.key", "AKIAIOSFDNN7EXAMPLEQ");
        lakefsConf.set("fs.lakefs.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        LakeFSClient lfsClient = new LakeFSClient("lakefs", lakefsConf);
        RepositoryCreation repoCreation = new RepositoryCreation();
        repoCreation.setName(String.format("test-spark-%s", UUID.randomUUID()));
        repoCreation.setStorageNamespace(String.format("%s/%s", storageNamespace, repoCreation.getName()));
        lfsClient.getRepositoriesApi().createRepository(repoCreation).execute();
        String catalog = "lakefs";
        String db = "db";
        String table = "mytable";
        String repo = repoCreation.getName();
        String mainBranch = "main";
        
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.catalog.LakeFSCatalog");
        conf.set("spark.sql.catalog.lakefs.warehouse", "lakefs://example-repo");
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

        conf.set("spark.hadoop.fs.s3a.access.key", awsAccessKey);
        conf.set("spark.hadoop.fs.s3a.secret.key", awsSecretKey);
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");
        conf.set("spark.hadoop.fs.lakefs.impl", "io.lakefs.LakeFSFileSystem");
        conf.set("spark.hadoop.fs.lakefs.access.key", "AKIAIOSFDNN7EXAMPLEQ");
        conf.set("spark.hadoop.fs.lakefs.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        conf.set("spark.hadoop.fs.lakefs.endpoint", "http://localhost:8000/api/v1");
        conf.set("spark.sql.catalog.lakefs.default-namespace", String.format("%s.%s", repo, mainBranch));

        SparkSession spark = SparkSession.builder().master("local").config(conf).getOrCreate();

        // Tests create namespace
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.`%s`.%s.%s", catalog, repo, mainBranch, db));

        // Test load namespace metadata
        spark.sql(String.format("DESCRIBE SCHEMA %s.`%s`.%s.%s", catalog, repo, mainBranch, db)).show();
        spark.sql(String.format("DESCRIBE SCHEMA %s.`%s`.%s", catalog, repo, mainBranch)).show();
        
        // Tests create table
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS  %s.`%s`.%s.%s.%s (val int) OPTIONS ('format-version'=2)", catalog,repo, mainBranch, db, table));
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("val", DataTypes.IntegerType, false)
        });

        Row row = RowFactory.create(10);
        Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema).toDF("val");
        
        // Tests TableOperations commit
        df.writeTo(String.format("%s.`%s`.%s.%s.%s", catalog, repo, mainBranch, db, table)).append();
        
        // Test list tables
        spark.sql(String.format("USE %s.`%s`.`%s`.%s", catalog, repo, mainBranch, db));
        spark.sql("SHOW TABLES").show(20, false);
        
        // Tests load table
        spark.sql(String.format("SELECT * FROM %s.`%s`.%s.%s.%s", catalog, repo, mainBranch, db, table)).show();

        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.`%s`.%s.test1", catalog, repo, mainBranch));
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.`%s`.%s.test2", catalog, repo, mainBranch));
        
        spark.sql("SHOW CURRENT NAMESPACE").show(20, false);

        // Try to delete non-empty namespace
        Assert.assertThrows(NamespaceNotEmptyException.class, () ->
                spark.sql(String.format("DROP SCHEMA %s.`%s`.%s.%s", catalog, repo, mainBranch, db)));
        
        // Delete Table and schema
        spark.sql(String.format("DROP TABLE %s.`%s`.%s.%s.%s", catalog,repo, mainBranch, db, table));
        spark.sql(String.format("DROP SCHEMA %s.`%s`.%s.%s", catalog, repo, mainBranch, db));
    }
}
