package io.lakefs.iceberg.catalog;

import io.lakefs.LakeFSClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.model.RepositoryCreation;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

public class TestLakeFSTableOperations {
    private String lakeFSRepo = "myrepo";
    private String lakeFSRef = "main";
    private String baseUrl ="";
    private Configuration conf;

    private LakeFSFileIO lakeFSFileIO;

    @Before
    public void setUp() throws IOException, ApiException {
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String storageNamespace = System.getenv("STORAGE_NAMESPACE");

        conf = new Configuration();
        conf.set("fs.lakefs.access.key", "AKIAIOSFDNN7EXAMPLEQ");
        conf.set("fs.lakefs.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        conf.set("fs.lakefs.endpoint", "http://localhost:8000/api/v1");
        conf.set("fs.s3a.access.key", awsAccessKey);
        conf.set("fs.s3a.secret.key", awsSecretKey);

        Configuration lakefsConf = new Configuration();
        lakefsConf.set("fs.lakefs.access.key", "AKIAIOSFDNN7EXAMPLEQ");
        lakefsConf.set("fs.lakefs.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        LakeFSClient lfsClient = new LakeFSClient("lakefs", lakefsConf);
        RepositoryCreation repoCreation = new RepositoryCreation();
        lakeFSRepo = String.format("myrepo-%s", UUID.randomUUID());
        repoCreation.setName(lakeFSRepo);
        baseUrl = String.format("lakefs://%s/%s", lakeFSRepo, lakeFSRef);
        repoCreation.setStorageNamespace(String.format("%s/%s", storageNamespace, repoCreation.getName()));
        lfsClient.getRepositoriesApi().createRepository(repoCreation).execute();
        lakeFSFileIO = new LakeFSFileIO(lakeFSRepo, lakeFSRef, conf);
    }

    @Test
    public void testNewInputFile() {
        Schema schema = new Schema(Types.NestedField.optional(0, "f1", new Types.BooleanType()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).build();
        TableMetadata md = TableMetadata.newTableMetadata(
                schema, spec, SortOrder.unsorted(), "my_location", ImmutableMap.of());
        TableMetadataParser.write(md, lakeFSFileIO.newOutputFile(baseUrl + "/mytable/v1.metadata.json"));
        Assert.assertThrows(AlreadyExistsException.class,() -> TableMetadataParser.write(md, lakeFSFileIO.newOutputFile(baseUrl + "/mytable/v1.metadata.json")));
    }
}
