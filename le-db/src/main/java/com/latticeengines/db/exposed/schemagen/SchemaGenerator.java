package com.latticeengines.db.exposed.schemagen;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.tool.hbm2ddl.SchemaExport;

public class SchemaGenerator {

    private static final Log log = LogFactory.getLog(SchemaGenerator.class);

    private Configuration cfg;
    private String schemaName = "";
    private DBDialect dialect;

    /**
     * an new instance is only good for one dialect. It seems that Hibernate
     * configuration cannot be reused by other dialect.
     *
     * @param name
     * @param packages
     *            - specific packages, not sub-package
     * @throws Exception
     */
    public SchemaGenerator(String name, DBDialect dialect, String... packages) throws Exception {
        cfg = new Configuration();
        this.dialect = dialect;
        init(name, packages);
        log.info("SchemaGenerator " + name + " for " + dialect + " dialect");
    }

    /**
     *
     * @param dbProp
     *            properties for database settings
     * @param packages
     * @throws Exception
     */
    public SchemaGenerator(String schemaName, Properties dbProp, DBDialect dialect, String... packages)
            throws Exception {
        cfg = new Configuration();
        cfg.setProperties(dbProp);
        this.dialect = dialect;
        init(schemaName, packages);
    }

    private void init(String name, String... packages) throws Exception {
        this.schemaName = name;
        cfg.setProperty("hibernate.hbm2ddl.auto", "create");
        cfg.setProperty("hibernate.globally_quoted_identifiers", "true");
        cfg.setProperty("connection.autocommit", "true");

        List<Class<?>> classes = new ArrayList<>();
        for (String packageName : packages) {
            classes.addAll(getClasses(packageName));
        }
        // error checking
        if (classes.isEmpty()) {
            throw new ClassNotFoundException("class not found for package: " + packages);
        }

        for (Class<?> clazz : classes) {
            cfg.addAnnotatedClass(clazz);
        }
    }

    private void generate(String outputFileName, boolean bScript, boolean bExportToDb) {
        cfg.setProperty("hibernate.dialect", dialect.getDialectClass());

        SchemaExport export = new SchemaExport(cfg);
        export.setDelimiter(";");
        export.setFormat(true);
        if (outputFileName != null) {
            export.setOutputFile(outputFileName);
        }

        export.execute(bScript, bExportToDb, false, false);
    }

    public void generateToScript() {
        generate("ddl_" + schemaName.toLowerCase() + "_" + dialect.name().toLowerCase() + ".sql", true, false);
    }

    public void generateToDatabase() {
        generate(null, false, true);
    }

    /**
     * SchemaGenerator - arguments[] <br>
     * [0] script only? - true (or empty) to indicate to generate script only,
     * false indicating to generate tables in database directly. <br>
     * [1] database configuration properties file path - this leverages the
     * dataplatform.properties for reusable settings <br>
     *
     * hibernate.connection.driver_class - JDBC driver class
     * hibernate.connection.url - JDBC URL hibernate.connection.username -
     * database user hibernate.connection.password - database user password
     * hibernate.connection.pool_size - maximum number of pooled connections
     *
     * @param args
     *            - [0]: scriptOnly - [1]: db properties
     *            (dataplatform.properties) file path
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int length = args.length;
        String dbName = args[0];

        String[] packages = new String[length - 1];

        int j = 0;
        for (int i = 1; i < length; i++) {
            packages[j++] = args[i];
        }
        SchemaGenerator gen = new SchemaGenerator(dbName, DBDialect.MYSQL5INNODB, packages);
        gen.generateToScript();
        gen = new SchemaGenerator(dbName, DBDialect.SQLSERVER, packages);
        gen.generateToScript();
    }

    private List<Class<?>> getClasses(String packageName) throws Exception {
        List<Class<?>> classes = new ArrayList<>();
        File directory = null;
        try {
            log.info("retreiving classes for package name:" + packageName);
            ClassLoader cld = Thread.currentThread().getContextClassLoader();
            if (cld == null) {
                throw new ClassNotFoundException("Can't get class loader.");
            }
            String packagePath = packageName.replace('.', '/');
            Enumeration<URL> resources = cld.getResources(packagePath);
            if (resources == null) {
                throw new ClassNotFoundException("No resources for " + packagePath);
            }

            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                log.debug("   url=" + url);
                directory = new File(url.getFile());

                if (directory.exists()) {
                    if (log.isDebugEnabled()) {
                        log.debug("   classes directory mode");
                    }

                    // deal with filesystem with classes case
                    String[] files = directory.list();
                    for (int i = 0; i < files.length; i++) {
                        if (files[i].endsWith(".class")) {
                            // removes the .class extension
                            String className = packageName + '.' + files[i].substring(0, files[i].length() - 6);
                            classes.add(Class.forName(className));
                            log.debug("adding class:" + className);
                        }
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("   jar classes mode");
                    }

                    // deal with the classes within jar files
                    // url=jar:file:/tmp/dataplatform/database/lib/le-domain-1.0.3-SNAPSHOT.jar!/com/latticeengines/domain/exposed/dataplatform
                    String[] paths = directory.getPath().split("!");
                    // strip off file:
                    File jarFilepath = new File(paths[0].substring(5));
                    JarFile jarFile = null;

                    try {
                        jarFile = new JarFile(jarFilepath, false, JarFile.OPEN_READ);
                        Enumeration<JarEntry> jarEntries = jarFile.entries();
                        while (jarEntries.hasMoreElements()) {
                            JarEntry jarEntry = jarEntries.nextElement();
                            // strip off file entry name
                            String jarPackage = jarEntry.getName().substring(0, jarEntry.getName().lastIndexOf('/'));
                            // only process specific package, not sub-package
                            if (!jarEntry.isDirectory() && jarPackage.equalsIgnoreCase(packageName.replace('.', '/'))) {
                                // remove .class extension
                                String fullyClassname = jarEntry.getName()
                                        .substring(0, jarEntry.getName().length() - 6);
                                classes.add(Class.forName(fullyClassname.replace('/', '.')));
                                if (log.isDebugEnabled()) {
                                    log.debug("adding class: " + fullyClassname);
                                }

                            }
                        }
                    } finally {
                        jarFile.close();
                    }
                }
            }
        } catch (NullPointerException x) {
            throw new ClassNotFoundException(packageName + " (" + directory + ") does not appear to be a valid package");
        }

        return classes;
    }

    private static enum DBDialect {
        /**
         * uses a custom dialect for SQLSERVER
         **/
        SQLSERVER("com.latticeengines.db.exposed.schemagen.LeSQLServer2008Dialect"), //
        MYSQL("org.hibernate.dialect.MySQLDialect"), //
        MYSQL5INNODB("org.hibernate.dialect.MySQL5InnoDBDialect"), //
        HSQL("org.hibernate.dialect.HSQLDialect");

        private String dialectClass;

        private DBDialect(String dialectClass) {
            this.dialectClass = dialectClass;
        }

        public String getDialectClass() {
            return dialectClass;
        }
    }
}