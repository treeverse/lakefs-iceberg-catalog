package io.lakefs.iceberg.catalog;

import io.lakefs.LakeFSFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * This is a Hadoop path with a dedicated {@link #toString()} implementation for lakeFS.
 * The path is assumed to be a lakeFS path of the form <code>[scheme]://[repo-name]/[ref-name]/rest/of/path/</code>.
 */
public class LakeFSPath extends Path {
    public LakeFSPath(String pathString) throws IllegalArgumentException {
        super(pathString);
        if (!pathString.startsWith("lakefs://")) {
            throw new IllegalArgumentException("Expecting a valid lakefs URI");
        }
    }

    /**
     * Return the path relative to the lakeFS repository and branch.
     * For example, given <code>lakefs://example-repo/main/a/b/c</code>, return <code>a/b/c</code>.
     */
    @Override
    public String toString() {
        return Util.GetPathFromURL(super.toString());
    }

    @Override
    public FileSystem getFileSystem(Configuration conf) throws IOException {
        try (LakeFSFileSystem fs = new LakeFSFileSystem()) {
            fs.initialize(toUri(), conf);
            return fs;
        }
        catch (IOException e) {
            throw new IOException(String.format("Failed to initialize lakeFS FileSystem with URI: %s", this.toUri().toString()), e);
        }
    }
}
