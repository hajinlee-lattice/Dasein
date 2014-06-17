package com.latticeengines.dataplatform.dao.impl;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.hibernate.cfg.Configuration;
import org.hibernate.tool.hbm2ddl.SchemaExport;

public class SchemaGenerator {
    private Configuration cfg;

    public SchemaGenerator(String... packages) throws Exception {
        cfg = new Configuration();
        cfg.setProperty("hibernate.hbm2ddl.auto", "create");
        cfg.setProperty("hibernate.globally_quoted_identifiers", "true");

        List<Class<?>> classes = new ArrayList<>();
        for (String packageName : packages) {
            classes.addAll(getClasses(packageName));
        }
        for (Class<?> clazz : classes) {
            cfg.addAnnotatedClass(clazz);
        }
    }

    private void generate(Dialect dialect) {
        generate(dialect, "ddl_" + dialect.name().toLowerCase() + ".sql");
    }
    
    private void generate(Dialect dialect, String outputFileName) {
        cfg.setProperty("hibernate.dialect", dialect.getDialectClass());

        SchemaExport export = new SchemaExport(cfg);
        export.setDelimiter(";");
        export.setFormat(true);
        export.setOutputFile(outputFileName);
        export.execute(true, false, false, false);
    }

    public static void main(String[] args) throws Exception {
        SchemaGenerator gen = new SchemaGenerator("com.latticeengines.domain.exposed.dataplatform",
                "com.latticeengines.domain.exposed.dataplatform.algorithm");
        gen.generate(Dialect.MYSQL);
        gen.generate(Dialect.SQLSERVER);
        gen.generate(Dialect.HSQL);
        
        SchemaGenerator genDlOrchestration = new SchemaGenerator("com.latticeengines.domain.exposed.dataplatform.dlorchestration",
                "com.latticeengines.domain.exposed.dataplatform.dlorchestration.hibernate");
        genDlOrchestration.generate(Dialect.MYSQL, "ddl_dlOrchestration_" + Dialect.MYSQL.name().toLowerCase() + ".sql");
        genDlOrchestration.generate(Dialect.SQLSERVER, "ddl_dlOrchestration_" + Dialect.SQLSERVER.name().toLowerCase() + ".sql");
        genDlOrchestration.generate(Dialect.HSQL, "ddl_dlOrchestration_" + Dialect.HSQL.name().toLowerCase() + ".sql");

    }

    private List<Class<?>> getClasses(String packageName) throws Exception {
        List<Class<?>> classes = new ArrayList<>();
        File directory = null;
        try {
            ClassLoader cld = Thread.currentThread().getContextClassLoader();
            if (cld == null) {
                throw new ClassNotFoundException("Can't get class loader.");
            }
            String path = packageName.replace('.', '/');
            Enumeration<URL> resources = cld.getResources(path);
            if (resources == null) {
                throw new ClassNotFoundException("No resources for " + path);
            }

            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                directory = new File(url.getFile());
                if (directory.exists()) {
                    String[] files = directory.list();
                    for (int i = 0; i < files.length; i++) {
                        if (files[i].endsWith(".class")) {
                            // removes the .class extension
                            classes.add(Class.forName(packageName + '.' + files[i].substring(0, files[i].length() - 6)));
                        }
                    }
                } else {
                    throw new ClassNotFoundException(packageName + " is not a valid package.");
                }
            }

        } catch (NullPointerException x) {
            throw new ClassNotFoundException(packageName + " (" + directory + ") does not appear to be a valid package");
        }

        return classes;
    }

    private static enum Dialect {
        SQLSERVER("org.hibernate.dialect.SQLServerDialect"), //
        MYSQL("org.hibernate.dialect.MySQLDialect"), //
        HSQL("org.hibernate.dialect.HSQLDialect");

        private String dialectClass;

        private Dialect(String dialectClass) {
            this.dialectClass = dialectClass;
        }

        public String getDialectClass() {
            return dialectClass;
        }
    }
}