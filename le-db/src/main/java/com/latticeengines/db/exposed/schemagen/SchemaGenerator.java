package com.latticeengines.db.exposed.schemagen;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.registry.internal.StandardServiceRegistryImpl;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.schema.Action;
import org.hibernate.tool.schema.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.schemagen.postprocess.MySQLPostProcessor;
import com.latticeengines.db.exposed.schemagen.postprocess.PostProcessor;
import com.latticeengines.db.exposed.schemagen.postprocess.SQLServerPostProcessor;
import com.latticeengines.db.naming.LEImplicitNamingStrategy;

public class SchemaGenerator {

    private static final Logger log = LoggerFactory.getLogger(SchemaGenerator.class);

    private String schemaName = "";
    private DBDialect dialect;
    private PostProcessor postProcessor;

    private ServiceRegistry serviceRegistry;
    private Metadata metadata;
    private Map<String, Object> hibernateProperties;

    /**
     * an new instance is only good for one dialect. It seems that Hibernate
     * configuration cannot be reused by other dialect.
     *
     * @param name
     * @param packages
     *            - specific packages, not sub-package
     * @throws Exception
     */
    public SchemaGenerator(String name, DBDialect dialect, PostProcessor postProcessor,
            String... packages) throws Exception {
        this.hibernateProperties = getConfigProperties(dialect);
        this.dialect = dialect;
        this.schemaName = name;
        this.postProcessor = postProcessor;
        init(packages);
        log.info("SchemaGenerator " + name + " for " + dialect + " dialect");
    }

    /**
     *
     * @param dbProp
     *            properties for database settings
     * @param packages
     * @throws Exception
     */
    /*
     * public SchemaGenerator(String schemaName, Properties dbProp, DBDialect
     * dialect, String... packages) throws Exception { cfg = new
     * Configuration();
     *
     * cfg.setProperties(dbProp); this.dialect = dialect; init(schemaName,
     * packages); }
     */

    private Map<String, Object> getConfigProperties(DBDialect dialect) {
        Map<String, Object> hibernateProperties = new HashMap<>();
        hibernateProperties.put(AvailableSettings.HBM2DDL_AUTO, Action.CREATE_DROP);
        hibernateProperties.put(AvailableSettings.GLOBALLY_QUOTED_IDENTIFIERS, true);
        hibernateProperties.put(AvailableSettings.AUTOCOMMIT, true);
        hibernateProperties.put(AvailableSettings.DIALECT, dialect.getDialectClass());
        hibernateProperties.put(AvailableSettings.IMPLICIT_NAMING_STRATEGY,
                LEImplicitNamingStrategy.INSTANCE);
        return hibernateProperties;
    }

    private void init(String... packages) throws Exception {
        this.serviceRegistry = new StandardServiceRegistryBuilder()
                .applySettings(this.hibernateProperties).build();

        List<Class<?>> classes = new ArrayList<>();
        for (String packageName : packages) {
            classes.addAll(getClasses(packageName));
        }
        // error checking
        if (classes.isEmpty()) {
            throw new ClassNotFoundException("class not found for package: " + packages);
        }

        MetadataSources mdSources = new MetadataSources(serviceRegistry);
        for (Class<?> clazz : classes) {
            mdSources.addAnnotatedClass(clazz);
        }
        this.metadata = mdSources.buildMetadata();
    }

    private void cleanupResources() {
        if (serviceRegistry != null)
            ((StandardServiceRegistryImpl) serviceRegistry).destroy();
    }

    private void generate(String outputFileName, boolean bScript, boolean bExportToDb) {

        try {
            SchemaExport export = new SchemaExport();
            export.setDelimiter(";");
            export.setFormat(false);
            if (outputFileName != null) {
                export.setOutputFile(outputFileName);
            }

            EnumSet<TargetType> targetTypes = EnumSet.noneOf(TargetType.class);
            if (bScript) {
                targetTypes.add(TargetType.SCRIPT);
            }
            if (bExportToDb) {
                targetTypes.add(TargetType.DATABASE);
            }
            export.execute(targetTypes, SchemaExport.Action.BOTH, this.metadata,
                    this.serviceRegistry);

        } finally {
            cleanupResources();
        }
    }

    public File generateToScript() throws IOException {
        String exportFileName = "ddl_" + schemaName.toLowerCase() + "_"
                + dialect.name().toLowerCase() + ".sql";
        generate(exportFileName, true, false);
        postProcess(exportFileName);

        log.info("Genetated Schema at: " + exportFileName);
        return new File(exportFileName);
    }

    private void postProcess(String exportFileName) {
        File file = new File(exportFileName);
        if (postProcessor != null) {
            postProcessor.process(file);
        }
    }

    public void appendStaticSql(File exportFile, DBDialect dbDialect) throws IOException {
        log.info("static sql is by passed.");
        return;

        // String leafFolder = "";
        // switch (dbDialect) {
        // case MYSQL5INNODB:
        // leafFolder = "mysql";
        // break;
        // case SQLSERVER:
        // leafFolder = "sqlserver";
        // break;
        // default:
        // break;
        // }
        // Iterable<File> iterable = Files.fileTreeTraverser()
        // .children(new File("src/main/resources/staticsql/" + leafFolder));
        // for (File f : iterable) {
        // if (f.getName().equals(".svn") || f.isDirectory()) {
        // continue;
        // }
        // log.info(String.format("appending %s to %s", f.getAbsolutePath(),
        // exportFile.getAbsolutePath()));
        // String staticSql = Files.toString(f, Charsets.UTF_8);
        // Files.append(staticSql, exportFile, Charsets.UTF_8);
        // }

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

        boolean withStaticSql = !("false".equals(args[1].toLowerCase()));

        String[] packages = withStaticSql ? new String[length - 1] : new String[length - 2];

        int j = 0;
        for (int i = args.length - packages.length; i < length; i++) {
            packages[j++] = args[i];
        }

        SchemaGenerator mysqlGenerator = new SchemaGenerator(dbName, DBDialect.MYSQL, null,
                packages);
        File mysqlExportFile = mysqlGenerator.generateToScript();

        SchemaGenerator mysql5Generator = new SchemaGenerator(dbName, DBDialect.MYSQL5INNODB,
                new MySQLPostProcessor(), packages);
        File mysql5ExportFile = mysql5Generator.generateToScript();

        SchemaGenerator sqlServerGenerator = new SchemaGenerator(dbName, DBDialect.SQLSERVER,
                new SQLServerPostProcessor(), packages);
        File sqlServerExportFile = sqlServerGenerator.generateToScript();

        if (withStaticSql) {
            mysqlGenerator.appendStaticSql(mysqlExportFile, DBDialect.MYSQL);
            mysql5Generator.appendStaticSql(mysql5ExportFile, DBDialect.MYSQL5INNODB);
            sqlServerGenerator.appendStaticSql(sqlServerExportFile, DBDialect.SQLSERVER);
        }
    }

    private List<Class<?>> getClasses(String packageName) throws Exception {
        List<Class<?>> classes = new ArrayList<>();
        File directory = null;
        try {
            log.info("retrieving classes for package name:" + packageName);
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
                            String className = packageName + '.'
                                    + files[i].substring(0, files[i].length() - 6);
                            classes.add(Class.forName(className));
                            log.debug("adding class:" + className);
                        }
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("   jar classes mode");
                    }

                    // deal with the classes within jar files
                    String[] paths = directory.getPath().split("!");
                    // strip off file:
                    File jarFilepath = new File(paths[0].substring(5));
                    JarFile jarFile = null;

                    try {
                        jarFile = new JarFile(jarFilepath, false, JarFile.OPEN_READ);
                        Enumeration<JarEntry> jarEntries = jarFile.entries();
                        while (jarEntries.hasMoreElements()) {
                            JarEntry jarEntry = jarEntries.nextElement();

                            if (!jarEntry.getName().endsWith(".class")) {
                                continue;
                            }
                            // strip off file entry name
                            String jarPackage = jarEntry.getName().substring(0,
                                    jarEntry.getName().lastIndexOf('/'));

                            // only process specific package, not sub-package
                            if (!jarEntry.isDirectory()
                                    && jarPackage.equalsIgnoreCase(packageName.replace('.', '/'))) {
                                // remove .class extension
                                String fullyClassname = jarEntry.getName().substring(0,
                                        jarEntry.getName().length() - 6);

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
            throw new ClassNotFoundException(
                    packageName + " (" + directory + ") does not appear to be a valid package");
        }

        return classes;
    }

    private enum DBDialect {
        /**
         * uses a custom dialect for SQLSERVER
         **/
        SQLSERVER("com.latticeengines.db.exposed.schemagen.LeSQLServer2008Dialect"), //
        MYSQL("org.hibernate.dialect.MySQL5Dialect"), //
        MYSQL5INNODB("org.hibernate.dialect.MySQL5InnoDBDialect"), //
        HSQL("org.hibernate.dialect.HSQLDialect");

        private String dialectClass;

        DBDialect(String dialectClass) {
            this.dialectClass = dialectClass;
        }

        public String getDialectClass() {
            return dialectClass;
        }
    }
}
