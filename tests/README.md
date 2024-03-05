# lakeFS Iceberg Tests

This package consists of the integration tests for the lakeFS Iceberg REST library.
Please make sure to add / modify tests whenever pushing new code to the SDK.

These tests can be run against a lakeFS instance with the following environment configuration:

## Python Tests Configuration

### Export lakectl environment variables

```sh
export LAKECTL_CREDENTIALS_ACCESS_KEY_ID=<your_lakefs_access_key_id>
export LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=<your_lakefs_secret_access_key>
export LAKECTL_SERVER_ENDPOINT_URL=<your_lakefs_endpoint>
```

### Export test environment variables

```sh
export STORAGE_NAMESPACE=<base storage namespace for your tests>
```

### Create a venv and install dependencies

From the `tests` folder:

```sh
python3 -m venv <your_venv_path>
source <your_venv_path>/bin/activate
pip install -r requirements pylint
```

### Run Tests

```sh
pylint tests
```

## Java Tests Configuration

### Export test environment variables

```sh
export AWS_ACCESS_KEY_ID: <your aws access key id>
export AWS_SECRET_ACCESS_KEY: <your aws secret access key>
export STORAGE_NAMESPACE=<base storage namespace for your tests>
```