<img src="https://docs.lakefs.io/assets/logo.svg" alt="lakeFS logo" width=300/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<img src="https://www.apache.org/logos/res/iceberg/iceberg.png" alt="Apache Iceberg logo" width=300/>

## lakeFS Iceberg Catalog

lakeFS enriches your Iceberg tables with Git capabilities: create a branch and make your changes in isolation, without affecting other team members.

See the instructions below on build, configuration and usage

## Build

From the repository root run the following maven command

```sh
mvn clean install -U -DskipTests
```

Under the `target` directory you will find the jar:

`lakefs-iceberg-catalog-<version>.jar`

Load this jar into your environment.

## Configuration

lakeFS Catalog is using lakeFS HadoopFileSystem under the hood to interact with lakeFS.
In addition, for better performance we configure the S3A FS to interact directly with the underlying storage:

```scala
conf.set("spark.hadoop.fs.lakefs.impl", "io.lakefs.LakeFSFileSystem")
conf.set("spark.hadoop.fs.lakefs.access.key", "AKIAIOSFDNN7EXAMPLEQ")
conf.set("spark.hadoop.fs.lakefs.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
conf.set("spark.hadoop.fs.lakefs.endpoint", "http://localhost:8000/api/v1")
conf.set("spark.hadoop.fs.s3a.access.key", "<your-aws-access-key>")
conf.set("spark.hadoop.fs.s3a.secret.key", "<your-aws-secret-key>")
```

In the catalog configuration pass the lakefs FS scheme configured previously as the warehouse location

```scala
conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog")
conf.set("spark.sql.catalog.lakefs.warehouse", "lakefs://")
```

## Usage

For our examples, assume lakeFS repository called `myrepo`.

### Create a table

Let's create a table called `table1` under `main` branch and namespace `name.space.`
To create the table, use the following syntax:

```sql
CREATE TABLE lakefs.myrepo.main.name.space.table1 (id int, data string);
```

### Create a branch

We will create a new branch `dev` from `main`, but first lets commit the creation of the table to the main branch:

```
lakectl commit lakefs://myrepo/main -m "my first iceberg commit"
```

To create a new branch:

```
lakectl branch create lakefs://myrepo/dev -s lakefs://myrepo/main
```

### Make changes on the branch

We can now make changes on `dev` branch:

```sql
INSERT INTO lakefs.myrepo.dev.name.space.table1 VALUES (3, 'data3');
```

### Query the table

If we query the table on the `dev` branch, we will see the data we inserted:

```sql
SELECT * FROM lakefs.myrepo.dev.name.space.table1;
```

Results in:
```
+----+------+
| id | data |
+----+------+
| 1  | data1|
| 2  | data2|
| 3  | data3|
+----+------+
```

However, data on the `main` branch remains unaffected:

```sql
SELECT * FROM lakefs.myrepo.main.name.space.table1;
```

Results in:
```
+----+------+
| id | data |
+----+------+
| 1  | data1|
| 2  | data2|
+----+------+
```
