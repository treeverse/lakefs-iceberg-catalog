package io.lakefs.iceberg.rest;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * LakeFSFileIO extends HadoopFileIO and provides support for relative lakeFS paths in context with a given 
 * repository and reference.
 * It also uses the LakeFSPath object to provide the relative path functionality for the Iceberg Catalog
 */
public class LakeFSFileIO extends HadoopFileIO {

    private transient Configuration conf; // transient - to avoid Spark serialization error
    private String lakeFSRepo;
    private String lakeFSRef;
    
    @SuppressWarnings("unused")
    public LakeFSFileIO() {
    }

    public LakeFSFileIO(String lakeFSRepo, String lakeFSRef, Configuration conf) {
        super(conf);
        this.conf = conf;
        this.lakeFSRepo = lakeFSRepo;
        this.lakeFSRef = lakeFSRef;
    }

    private String verifyPath(String path) throws IllegalArgumentException {
        if (!path.matches("^[0-9a-z]*://.*")) {
            path = String.format("%s%s/%s/%s", LakeFSCatalog.WAREHOUSE_LOCATION, lakeFSRepo, lakeFSRef, path);
        }
        if (!path.startsWith(String.format("%s%s/%s/", LakeFSCatalog.WAREHOUSE_LOCATION, lakeFSRepo, lakeFSRef))) {
            // Allow creating Files only under FileIO repository and ref
            throw new IllegalArgumentException("Wrong repository and reference provided");
        }
        return path;
    }
    
    @Override
    public InputFile newInputFile(String path) {
        path = verifyPath(path);
        return HadoopInputFile.fromPath(new LakeFSPath(path), conf);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        path = verifyPath(path);
        return HadoopInputFile.fromPath(new LakeFSPath(path), length, conf);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        path = verifyPath(path);
        return HadoopOutputFile.fromPath(new LakeFSPath(path), conf);
    }
}
