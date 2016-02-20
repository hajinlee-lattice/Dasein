package com.latticeengines.sqoop.csvimport.mapreduce;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.mapreduce.AvroJob;
import org.apache.sqoop.mapreduce.AvroOutputFormat;
import org.apache.sqoop.mapreduce.DBWritable;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.ImportJobBase;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce.LedpCSVToAvroImportMapper;
import com.latticeengines.sqoop.csvimport.mapreduce.db.CSVDBInputFormat;

@SuppressWarnings("deprecation")
public class CSVImportJob extends ImportJobBase {

    public CSVImportJob(final SqoopOptions opts) {
        super(opts, null, CSVDBInputFormat.class, null, null);
    }

    @SuppressWarnings("rawtypes")
    public CSVImportJob(final SqoopOptions opts, final Class<? extends InputFormat> inputFormatClass,
            ImportJobContext context) {
        super(opts, null, inputFormatClass, null, context);
    }

    @Override
    protected void configureMapper(Job job, String tableName, String tableClassName) throws IOException {
        LOG.info("Using ledp CSVImportJob");
        if (job.getConfiguration().get("avro.schema") != null) {
            Schema ledpSchema = Schema.parse(job.getConfiguration().get("avro.schema"));
            AvroJob.setMapOutputSchema(job.getConfiguration(), ledpSchema);
        } else {
            throw new RuntimeException("avro.schema is not set!!");
        }

        LOG.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        LOG.info("ImportSource is:" + job.getConfiguration().get("importMapperClass"));
        if (job.getConfiguration().get("importMapperClass") != null) {
            job.setMapperClass(LedpCSVToAvroImportMapper.class);
        } else {
            throw new RuntimeException("importMapperClass is not set!!");
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Class<? extends Mapper> getMapperClass() {
        if (options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
            return LedpCSVToAvroImportMapper.class;
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Class<? extends OutputFormat> getOutputFormatClass() throws ClassNotFoundException {
        if (options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
            return AvroOutputFormat.class;
        }
        return null;
    }

    @Override
    protected void configureInputFormat(Job job, String tableName, String tableClassName, String splitByCol)
            throws IOException {
        ConnManager mgr = getContext().getConnManager();
        try {
            String username = options.getUsername();
            if (null == username || username.length() == 0) {
                DBConfiguration.configureDB(job.getConfiguration(), mgr.getDriverClass(), options.getConnectString(),
                        options.getFetchSize(), options.getConnectionParams());
            } else {
                DBConfiguration.configureDB(job.getConfiguration(), mgr.getDriverClass(), options.getConnectString(),
                        username, options.getPassword(), options.getFetchSize(), options.getConnectionParams());
            }

            if (null != tableName) {
                // Import a table.
                String[] colNames = options.getColumns();
                if (null == colNames) {
                    colNames = mgr.getColumnNames(tableName);
                }

                String[] sqlColNames = null;
                if (null != colNames) {
                    sqlColNames = new String[colNames.length];
                    for (int i = 0; i < colNames.length; i++) {
                        sqlColNames[i] = mgr.escapeColName(colNames[i]);
                    }
                }

                // It's ok if the where clause is null in
                // DBInputFormat.setInput.
                String whereClause = options.getWhereClause();

                // We can't set the class properly in here, because we may not
                // have the
                // jar loaded in this JVM. So we start by calling setInput()
                // with
                // DBWritable and then overriding the string manually.
                CSVDBInputFormat.setInput(job, DBWritable.class, mgr.escapeTableName(tableName), whereClause,
                        mgr.escapeColName(splitByCol), sqlColNames);

                // If user specified boundary query on the command line
                // propagate it to
                // the job
                if (options.getBoundaryQuery() != null) {
                    CSVDBInputFormat.setBoundingQuery(job.getConfiguration(), options.getBoundaryQuery());
                }
            } else {
                // Import a free-form query.
                String inputQuery = options.getSqlQuery();
                String sanitizedQuery = inputQuery.replace(CSVDBInputFormat.SUBSTITUTE_TOKEN, " (1 = 1) ");

                String inputBoundingQuery = options.getBoundaryQuery();
                if (inputBoundingQuery == null) {
                    inputBoundingQuery = buildBoundaryQuery(splitByCol, sanitizedQuery);
                }
                CSVDBInputFormat.setInput(job, DBWritable.class, inputQuery, inputBoundingQuery);
                new DBConfiguration(job.getConfiguration()).setInputOrderBy(splitByCol);
            }
            if (options.getRelaxedIsolation()) {
                LOG.info("Enabling relaxed (read uncommitted) transaction " + "isolation for imports");
                job.getConfiguration().setBoolean(DBConfiguration.PROP_RELAXED_ISOLATION, true);
            }
            LOG.debug("Using table class: " + tableClassName);
            job.getConfiguration().set(ConfigurationHelper.getDbInputClassProperty(), tableClassName);

            job.getConfiguration().setLong(LargeObjectLoader.MAX_INLINE_LOB_LEN_KEY, options.getInlineLobLimit());

            LOG.info("Using InputFormat: " + inputFormatClass);
            job.setInputFormatClass(inputFormatClass);
        } finally {
            try {
                mgr.close();
            } catch (SQLException sqlE) {
                LOG.warn("Error closing connection: " + sqlE);
            }
        }
    }

    private String buildBoundaryQuery(String col, String query) {
        if (col == null || options.getNumMappers() == 1) {
            return "";
        }

        // Replace table name with alias 't1' if column name is a fully
        // qualified name. This is needed because "tableName"."columnName"
        // in the input boundary query causes a SQL syntax error in most dbs
        // including Oracle and MySQL.
        String alias = "t1";
        int dot = col.lastIndexOf('.');
        String qualifiedName = (dot == -1) ? col : alias + col.substring(dot);

        ConnManager mgr = getContext().getConnManager();
        String ret = mgr.getInputBoundsQuery(qualifiedName, query);
        if (ret != null) {
            return ret;
        }

        return "SELECT MIN(" + qualifiedName + "), MAX(" + qualifiedName + ") " + "FROM (" + query + ") AS " + alias;
    }

}
