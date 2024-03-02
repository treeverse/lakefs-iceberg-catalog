package io.lakefs.iceberg.catalog;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.util.*;

import com.amazonaws.thirdparty.jackson.databind.ObjectMapper;
import io.lakefs.LakeFSFileStatus;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LakeFSCatalog provides a way to use table names like repo_name.branch_name.table to work with path-based tables
 * under a common location.
 */
public class LakeFSCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {
    // Most of the code taken for HadoopCatalog with some modifications in regard to the FileSystem and path operations.
    // It uses a specified directory under the lakeFS FileSystem as the warehouse directory, 
    // and organizes multiple levels directories that mapped to the database, namespace and the table respectively.
    
    private static final Logger LOG = LoggerFactory.getLogger(LakeFSCatalog.class);
    
    private static final String TABLE_METADATA_FILE_EXTENSION = ".metadata.json";
    public static final String NAMESPACE_FILENAME = "ns_metadata.json";
    private static final PathFilter TABLE_FILTER =
            path -> path.getName().endsWith(TABLE_METADATA_FILE_EXTENSION);
    private static final String HADOOP_SUPPRESS_PERMISSION_ERROR = "suppress-permission-error";

    public static final String WAREHOUSE_LOCATION = "lakefs://";

    private String catalogName;
    private Configuration conf;
    private String warehouseLocation;
    private boolean suppressPermissionError = false;
    private Map<String, String> catalogProperties;

    @Override
    public void initialize(String name, Map<String, String> properties) {
        catalogProperties = ImmutableMap.copyOf(properties);
        String inputWarehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
        Preconditions.checkArgument(
                inputWarehouseLocation != null && !inputWarehouseLocation.isEmpty(),
                String.format("Missing catalog property %s. Cannot initialize LakeFSCatalog because " +
                        "warehousePath must not be null or empty", CatalogProperties.WAREHOUSE_LOCATION));

        catalogName = name;
        warehouseLocation = WAREHOUSE_LOCATION;
        suppressPermissionError = Boolean.parseBoolean(properties.get(HADOOP_SUPPRESS_PERMISSION_ERROR));
        // TODO (niro): Future - create a caching mechanism for FileSystem Initialization per repo
    }

    @Override
    public String name() {
        return catalogName;
    }

    private boolean shouldSuppressPermissionError(IOException ioException) {
        if (suppressPermissionError) {
            return ioException instanceof AccessDeniedException || 
                    ioException.getMessage() != null && 
                            ioException.getMessage().contains("AuthorizationPermissionMismatch");
        }
        return false;
    }

    private boolean isTableDir(Path path) {
        Path metadataPath = new Path(path, "metadata");
        // Only the path which contains metadata is the path for table, otherwise it could be
        // still a namespace.
        try {
            FileSystem fs = path.getFileSystem(conf);
            return fs.listStatus(metadataPath, TABLE_FILTER).length >= 1; // TODO (niro): use fs.listStatusIterator instead
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            if (shouldSuppressPermissionError(e)) {
                LOG.warn("Unable to list metadata directory {}", metadataPath, e);
                return false;
            }
            throw new UncheckedIOException(e);
        }
    }

    private boolean isDirectory(Path path) {
        try {
            FileSystem fs = path.getFileSystem(conf);
            return fs.getFileStatus(path).isDirectory();
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            if (shouldSuppressPermissionError(e)) {
                LOG.warn("Unable to list directory {}", path, e);
                return false;
            } else {
                LOG.error("Unable to list directory {}", path, e);
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        Preconditions.checkArgument(
                namespace.levels().length > 1, "Missing database in table identifier: %s", namespace);

        String location = String.format("%s%s", WAREHOUSE_LOCATION, defaultWarehouseLocation(namespace));
        Set<TableIdentifier> tblIdents = Sets.newHashSet();
        try {
            Path nsPath = new Path(new URI(location));
            FileSystem fs = nsPath.getFileSystem(conf);
            if (!isDirectory(nsPath)) {
                throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
            }
            RemoteIterator<FileStatus> it = fs.listStatusIterator(nsPath);
            while (it.hasNext()) {
                FileStatus status = it.next();
                if (!status.isDirectory()) {
                    // Ignore the path which is not a directory.
                    continue;
                }

                Path path = status.getPath();
                if (isTableDir(path)) {
                    TableIdentifier tblIdent = TableIdentifier.of(namespace, path.getName());
                    tblIdents.add(tblIdent);
                }
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(String.format("Failed to list tables under: %s", namespace), ioe);
        } catch (URISyntaxException e) {
            LOG.error(String.format("Failed to parse URI: %s", location));
            throw new RuntimeException(e);
        }

        return Lists.newArrayList(tblIdents);
    }

    @Override
    protected boolean isValidIdentifier(TableIdentifier identifier) {
        return true;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier identifier) {
        final String[] levels = identifier.namespace().levels();
        Preconditions.checkArgument(levels.length > 2, String.format("Missing database in table identifier: %s", identifier));
        Configuration conf = getConf();
        LakeFSFileIO fileIO = new LakeFSFileIO(levels[0], levels[1], conf);
        String location = String.format("%s%s", warehouseLocation, defaultWarehouseLocation(identifier));
        return new LakeFSTableOperations(new Path(location), fileIO, conf);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        String tableName = tableIdentifier.name();
        return defaultWarehouseLocation(tableIdentifier.namespace()) + tableName;
    }

    protected String defaultWarehouseLocation(Namespace ns) {
        return String.join("/", ns.levels()) + "/";
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        if (!isValidIdentifier(identifier)) {
            throw new NoSuchTableException("Invalid identifier: %s", identifier);
        }

        String location = String.format("%s%s", WAREHOUSE_LOCATION, defaultWarehouseLocation(identifier));
        Path tablePath;
        try {
            tablePath = new Path(new URI(location));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        TableOperations ops = newTableOps(identifier);
        TableMetadata lastMetadata = ops.current();
        if (lastMetadata == null) {
            LOG.debug("Not an iceberg table: {}", identifier);
            return false;
        }
        try {
            FileSystem fs = tablePath.getFileSystem(conf);
            if (purge) {
                    // Since the data files and the metadata files may store in different locations,
                    // so it has to call dropTableData to force delete the data file.
                    CatalogUtil.dropTableData(ops.io(), lastMetadata);
            }
            return fs.delete(tablePath, true /* recursive */);
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("Failed to delete file: %s", tablePath), e);
        }
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        throw new UnsupportedOperationException("Cannot rename lakeFS Iceberg tables");
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        Preconditions.checkArgument(
                !namespace.isEmpty(), "Cannot create namespace with invalid name: %s", namespace);
        String location = String.format("%s%s", WAREHOUSE_LOCATION, defaultWarehouseLocation(namespace));
        Path metadataPath = new Path(location + "/" + NAMESPACE_FILENAME);
        Path nsPath;
        try {
            nsPath = new Path(new URI(location));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        if (isNamespace(nsPath)) {
            throw new AlreadyExistsException("Namespace already exists: %s", namespace);
        }

        try {
            FileSystem fs = metadataPath.getFileSystem(conf);
            FSDataOutputStream stream = fs.create(metadataPath, false);
            ObjectMapper mapper = new ObjectMapper();
            stream.write(mapper.writeValueAsBytes(meta));
            stream.close();
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("Create namespace failed: %s", namespace), e);
        }
    }

    @Override
    public List<Namespace> listNamespaces() {
        throw new UnsupportedOperationException("Top-level listing not supported");
    }
    
    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        if (namespace.length() < 2) {
            throw new NoSuchNamespaceException("Namespace must contain at least repository and branch levels: %s", namespace);
        }

        String location = String.format("%s%s", WAREHOUSE_LOCATION, defaultWarehouseLocation(namespace));
        Path nsPath;
        try {
            nsPath = new Path(new URI(location));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        if (!isNamespace(nsPath)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        try {
            FileSystem fs = nsPath.getFileSystem(conf);
            // using the iterator listing allows for paged downloads
            // from lakeFS and prefetching from object storage.
            List<Namespace> namespaces = Lists.newArrayList();
            RemoteIterator<FileStatus> it = fs.listStatusIterator(nsPath);
            while (it.hasNext()) {
                Path path = it.next().getPath();
                if (isNamespace(path)) {
                    namespaces.add(append(namespace, path.getName()));
                }
            }
            return namespaces;
        } catch (IOException ioe) {
            throw new UncheckedIOException(String.format("Failed to list namespace under: %s", namespace), ioe);
        }
    }

    private Namespace append(Namespace ns, String name) {
        String[] levels = Arrays.copyOfRange(ns.levels(), 0, ns.levels().length + 1);
        levels[ns.levels().length] = name;
        return Namespace.of(levels);
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
        String location = String.format("%s%s", WAREHOUSE_LOCATION, defaultWarehouseLocation(namespace));
        // This method of getting the path removes the last slash so that the namespace directory is removed
        Path nsPath = new Path(location);

        if (!isNamespace(nsPath) || namespace.length() < 2) {
            return false;
        }

        try {
            FileSystem fs = nsPath.getFileSystem(conf);
            RemoteIterator<FileStatus> it = fs.listStatusIterator(nsPath);
            while (it.hasNext()) {
                LakeFSFileStatus status = (LakeFSFileStatus)it.next();
                if (!(status.isEmptyDirectory() || status.getPath().getName().equals(NAMESPACE_FILENAME))) {
                    throw new NamespaceNotEmptyException("Namespace %s is not empty.", namespace);
                }
            }
            return fs.delete(nsPath, true /* recursive */);
        } catch (IOException e) { 
            throw new UncheckedIOException(String.format("Namespace delete failed: %s", namespace), e);
        }
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        throw new UnsupportedOperationException(
                "Cannot set namespace properties " + namespace + " : setProperties is not supported");
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        throw new UnsupportedOperationException(
                "Cannot remove properties " + namespace + " : removeProperties is not supported");
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        Map<String,String> result = new HashMap<>();
        String location = String.format("%s%s", WAREHOUSE_LOCATION, defaultWarehouseLocation(namespace));
        Path nsPath = new Path(location);
        if (!isNamespace(nsPath) || namespace.isEmpty()) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        try {
            FileSystem fs = nsPath.getFileSystem(conf);
            Path mdPath = new Path(nsPath, NAMESPACE_FILENAME);
            if (fs.exists(mdPath)) {
                ObjectMapper mapper = new ObjectMapper();
                FSDataInputStream is = fs.open(mdPath);
                result = mapper.readValue((InputStream) is, Map.class);
                is.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        result.put("location", nsPath.toString());

        return result;
    }

    private boolean isNamespace(Path path) {
        return isDirectory(path) && !isTableDir(path);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", catalogName)
                .add("location", warehouseLocation)
                .toString();
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
        return new LakeFSCatalogTableBuilder(identifier, schema);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    protected Map<String, String> properties() {
        return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
    }

    private class LakeFSCatalogTableBuilder extends BaseMetastoreCatalogTableBuilder {
        private final String defaultLocation;

        private LakeFSCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
            super(identifier, schema);
            defaultLocation = Util.getPathFromURL(String.format("%s%s", WAREHOUSE_LOCATION, defaultWarehouseLocation(identifier)));
            super.withLocation(defaultLocation);
        }

        @Override
        public TableBuilder withLocation(String location) {
            Preconditions.checkArgument(
                    location == null || location.equals(defaultLocation),
                    "Cannot set a custom location for a path-based table. Expected "
                            + defaultLocation
                            + " but got "
                            + location);
            return this;
        }
    }
}
