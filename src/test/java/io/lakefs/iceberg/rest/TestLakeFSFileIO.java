package io.lakefs.iceberg.rest;

import io.lakefs.FSTestBase;
import org.apache.iceberg.io.InputFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLakeFSFileIO extends FSTestBase {

    private LakeFSFileIO lakeFSFileIO;
    private final String lakeFSRepo = "myLakeFSRepo";
    private final String lakeFSRef = "myLakeFSRef";


    @Before
    public void setUp(){
        lakeFSFileIO = new LakeFSFileIO(lakeFSRepo, lakeFSRef, conf);
    }

    @Test
    public void testNewInputFile() {
        // Test the behavior of newInputFile method
        String relativePath = "path/in/repo";
        String absolutePath = String.format("lakefs://%s/%s/other/path/in/repo", lakeFSRepo, lakeFSRef);
        String wrongRef = String.format("lakefs://%s/wrongRef/some/path", lakeFSRepo);
        String wrongRepo = String.format("lakefs://wrongRepo/%s/some/path", lakeFSRef);
        String wrongSchema = "s3a://otherBucket/otherPath";
        InputFile relativeInputFile = lakeFSFileIO.newInputFile(relativePath);
        Assert.assertEquals("path/in/repo", relativeInputFile.location());
        InputFile absoluteInputFile = lakeFSFileIO.newInputFile(absolutePath);
        Assert.assertEquals("other/path/in/repo", absoluteInputFile.location());
        Assert.assertThrows(IllegalArgumentException.class, () -> lakeFSFileIO.newInputFile(wrongRef));
        Assert.assertThrows(IllegalArgumentException.class, () -> lakeFSFileIO.newInputFile(wrongRepo));
        Assert.assertThrows(IllegalArgumentException.class, () -> lakeFSFileIO.newInputFile(wrongSchema));
    }
}
