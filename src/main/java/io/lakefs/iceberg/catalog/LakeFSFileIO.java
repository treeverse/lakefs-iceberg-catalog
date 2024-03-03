package io.lakefs.iceberg.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

    private final transient Configuration conf; // transient - to avoid Spark serialization error
    private final String basePath;

    public LakeFSFileIO(String warehouse, String lakeFSRepo, String lakeFSRef, Configuration conf) {
        super(conf);
        this.conf = conf;
        this.basePath = String.format("%s%s/%s", warehouse, lakeFSRepo, lakeFSRef);
        
    }

    private String verifyPath(String path) throws IllegalArgumentException {
        Path p = new Path(basePath, path);
        if (!p.toString().startsWith(basePath)) {
            // Allow creating Files only under FileIO repository and ref
            throw new IllegalArgumentException("Wrong repository and reference provided");
        }
        return p.toString();
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

    private static class LakeFSPath extends Path {
        public LakeFSPath(String pathString) throws IllegalArgumentException {
            super(pathString);
        }

        /**
         * Return the path relative to the lakeFS repository and branch.
         * For example, given <code>lakefs://example-repo/main/a/b/c</code>, return <code>a/b/c</code>.
         */
        @Override
        public String toString() {
            return Util.getPathFromURL(super.toString());
        }
    }
}
