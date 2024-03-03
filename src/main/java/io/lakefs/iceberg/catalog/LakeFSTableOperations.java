package io.lakefs.iceberg.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.io.FileIO;
import com.google.common.base.Preconditions;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * LakeFSTableOperations extends HadoopTableOperations.
 * It overrides the commit method to use an optimistic concurrency mechanism using lakeFS's Set-If-Absent to avoid 
 * using a locking mechanism
 * Most code is taken from the HadoopTableOperations in order to properly implement the commit method.
 */
public class LakeFSTableOperations extends HadoopTableOperations {
    private static final Logger LOG = LoggerFactory.getLogger(LakeFSTableOperations.class);
    private static final Pattern VERSION_PATTERN = Pattern.compile("v([^.]*)\\..*");
    private final Path location;
    private final String warehouse;
    private final FileSystem fs;
    private final FileIO fileIO;

    // Ensure reading currentMetadata will provide the last value written
    private volatile TableMetadata currentMetadata = null;
    // Will ensure the version read is the most up to date write
    private volatile Integer version = null;
    // volatile parameter to ensure that if it was set to true, it will be read appropriately  
    private volatile boolean shouldRefresh = true;

    public LakeFSTableOperations(Path location, FileIO fileIO, String warehouse, Configuration conf) {
        super(location, fileIO, conf, LockManagers.defaultLockManager());
        this.fileIO = fileIO;
        this.location = location;
        this.warehouse = warehouse;
        try {
            this.fs = location.getFileSystem(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FileIO io() {
        return fileIO;
    }


    @Override
    public String metadataFileLocation(String fileName) {
        String path = super.metadataFileLocation(fileName);
        if (path.startsWith(warehouse)) {
            path = Util.getPathFromURL(path);
        }
        return path;
    }

    @Override
    public TableMetadata current() {
        if (shouldRefresh) {
            return refresh();
        }
        return currentMetadata;
    }
    
    @Override
    public TableMetadata refresh() {
        int ver = version != null ? version : findVersion();
        try {
            Path metadataFile = getMetadataFile(ver);
            if (version == null && metadataFile == null && ver == 0) {
                // no v0 metadata means the table doesn't exist yet
                return null;
            } else if (metadataFile == null) {
                throw new ValidationException("Metadata file for version %d is missing", ver);
            }

            Path nextMetadataFile = getMetadataFile(ver + 1);
            while (nextMetadataFile != null) {
                ver += 1;
                metadataFile = nextMetadataFile;
                nextMetadataFile = getMetadataFile(ver + 1);
            }

            updateVersionAndMetadata(ver, metadataFile.toString());

            this.shouldRefresh = false;
            return currentMetadata;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to refresh the table", e);
        }
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        Pair<Integer, TableMetadata> current = versionAndMetadata();
        if (base != current.second()) {
            throw new CommitFailedException("Cannot commit changes based on stale table metadata");
        }

        if (base == metadata) {
            LOG.info("Nothing to commit.");
            return;
        }

        Preconditions.checkArgument(
                base == null || base.location().equals(metadata.location()),
                "lakeFS path-based tables cannot be relocated");
        Preconditions.checkArgument(
                !metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION),
                "lakeFS path-based tables cannot relocate metadata");

        String codecName =
                metadata.property(
                        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
        TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromName(codecName);
        int nextVersion = (current.first() != null ? current.first() : 0) + 1;
        Path metadataFile = metadataFilePath(nextVersion, codec);
        TableMetadataParser.write(metadata, io().newOutputFile(metadataFile.toString()));

        LOG.info("Committed a new metadata file {}", metadataFile);

        // update the best-effort version pointer
        writeVersionHint(nextVersion);

        this.shouldRefresh = true;
    }

    Path getMetadataFile(int metadataVersion) throws IOException {
        for (TableMetadataParser.Codec codec : TableMetadataParser.Codec.values()) {
            Path metadataFile = metadataFilePath(metadataVersion, codec);
            if (fs.exists(metadataFile)) {
                return metadataFile;
            }

            if (codec.equals(TableMetadataParser.Codec.GZIP)) {
                // we have to be backward-compatible with .metadata.json.gz files
                metadataFile = oldMetadataFilePath(metadataVersion, codec);
                if (fs.exists(metadataFile)) {
                    return metadataFile;
                }
            }
        }

        return null;
    }

    private synchronized Pair<Integer, TableMetadata> versionAndMetadata() {
        return Pair.of(version, currentMetadata);
    }

    private synchronized void updateVersionAndMetadata(int newVersion, String metadataFile) {
        // update if the current version is out of date
        if (version == null || version != newVersion) {
            this.currentMetadata =
                    checkUUID(currentMetadata, TableMetadataParser.read(io(), metadataFile));
            this.version = newVersion;
        }
    }

    private Path metadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
        return metadataPath("v" + metadataVersion + TableMetadataParser.getFileExtension(codec));
    }

    private Path oldMetadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
        return metadataPath("v" + metadataVersion + TableMetadataParser.getOldFileExtension(codec));
    }

    private Path metadataPath(String filename) {
        return new Path(metadataRoot(), filename);
    }

    private Path metadataRoot() {
        return new Path(location, "metadata");
    }

    private int version(String fileName) {
        Matcher matcher = VERSION_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            return -1;
        }
        String versionNumber = matcher.group(1);
        try {
            return Integer.parseInt(versionNumber);
        } catch (NumberFormatException ne) {
            return -1;
        }
    }
    
    Path versionHintFile() {
        return metadataPath(org.apache.iceberg.hadoop.Util.VERSION_HINT_FILENAME);
    }

    private void writeVersionHint(int versionToWrite) {
        Path versionHintFile = versionHintFile();

        try {
            Path tempVersionHintFile = metadataPath(UUID.randomUUID() + "-version-hint.temp");
            writeVersionToPath(fs, tempVersionHintFile, versionToWrite);
            fs.delete(versionHintFile, false /* recursive delete */);
            fs.rename(tempVersionHintFile, versionHintFile);
        } catch (IOException e) {
            LOG.warn("Failed to update version hint", e);
        }
    }

    private void writeVersionToPath(FileSystem fs, Path path, int versionToWrite) throws IOException {
        try (FSDataOutputStream out = fs.create(path, false /* overwrite */)) {
            out.write(String.valueOf(versionToWrite).getBytes(StandardCharsets.UTF_8));
        }
    }
    
    private int findVersion() {
        Path versionHintFile = versionHintFile();

        try (InputStreamReader fsr =
                     new InputStreamReader(fs.open(versionHintFile), StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(fsr)) {
            return Integer.parseInt(in.readLine().replace("\n", ""));
        } catch (Exception e) {
            try {
                if (fs.exists(metadataRoot())) {
                    LOG.warn("Error reading version hint file {}", versionHintFile, e);
                } else {
                    LOG.debug("Metadata for table not found in directory {}", metadataRoot(), e);
                    return 0;
                }

                // List the metadata directory to find the version files, and try to recover the max
                // available version
                FileStatus[] files =
                        fs.listStatus(
                                metadataRoot(), name -> VERSION_PATTERN.matcher(name.getName()).matches());
                int maxVersion = 0;

                for (FileStatus file : files) {
                    int currentVersion = version(file.getPath().getName());
                    if (currentVersion > maxVersion && getMetadataFile(currentVersion) != null) {
                        maxVersion = currentVersion;
                    }
                }

                return maxVersion;
            } catch (IOException io) {
                LOG.warn("Error trying to recover version-hint.txt data for {}", versionHintFile, e);
                return 0;
            }
        }
    }

    private static TableMetadata checkUUID(TableMetadata currentMetadata, TableMetadata newMetadata) {
        String newUUID = newMetadata.uuid();
        if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
            Preconditions.checkState(
                    newUUID.equals(currentMetadata.uuid()),
                    "Table UUID does not match: current=%s != refreshed=%s",
                    currentMetadata.uuid(),
                    newUUID);
        }
        return newMetadata;
    }
}
