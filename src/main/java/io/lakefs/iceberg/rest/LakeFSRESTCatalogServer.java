package io.lakefs.iceberg.rest;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeFSRESTCatalogServer extends RESTCatalogAdapter{
  private static final Logger LOG = LoggerFactory.getLogger(LakeFSRESTCatalogServer.class);
  private static final String CATALOG_ENV_PREFIX = "CATALOG_";
  private  static final int DEFAULT_SERVER_PORT = 8181;
  private static final String CATALOG_NAME = "lakefs";

  public LakeFSRESTCatalogServer(Catalog catalog) {
    super(catalog);
  }

  private static Catalog backendCatalog() {
    // Translate environment variable to catalog properties
    Map<String, String> catalogProperties =
            System.getenv().entrySet().stream()
                    .filter(e -> e.getKey().startsWith(CATALOG_ENV_PREFIX))
                    .collect(
                            Collectors.toMap(
                                    e ->
                                            e.getKey()
                                                    .replaceFirst(CATALOG_ENV_PREFIX, "")
                                                    .replaceAll("__", "-")
                                                    .replaceAll("_", ".")
                                                    .toLowerCase(Locale.ROOT),
                                    Map.Entry::getValue,
                                    (m1, m2) -> {
                                      throw new IllegalArgumentException("Duplicate key: " + m1);
                                    },
                                    HashMap::new));


    catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, LakeFSCatalog.WAREHOUSE_LOCATION);
    LOG.info("Creating catalog with properties: {}", catalogProperties);
    LakeFSCatalog backendCatalog = new LakeFSCatalog();
    backendCatalog.initialize(CATALOG_NAME, catalogProperties);
    Configuration conf = new Configuration();
    backendCatalog.setConf(conf);
    return backendCatalog;
  }

  public static void main(String[] args) throws Exception {
    Catalog catalog = backendCatalog();

    try (RESTCatalogAdapter adapter = new RESTCatalogAdapter(catalog)) {
      RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);

      ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
      context.setContextPath("/");
      ServletHolder servletHolder = new ServletHolder(servlet);
      servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
      context.addServlet(servletHolder, "/*");
      context.setVirtualHosts(null);
      context.setGzipHandler(new GzipHandler());

      Server httpServer =
          new Server(PropertyUtil.propertyAsInt(System.getenv(), "REST_PORT", DEFAULT_SERVER_PORT));
      httpServer.setHandler(context);

      httpServer.start();
      httpServer.join();
    }
  }
}
