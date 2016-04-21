package com.latticeengines.sqoop.csvimport.mapreduce.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.util.LoggingUtils;
import org.apache.hadoop.mapreduce.Counter;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat.DBInputSplit;

import org.apache.sqoop.mapreduce.db.DataDrivenDBRecordReader;

@SuppressWarnings("deprecation")
public class CSVDBRecordReader<T extends DBWritable> extends DataDrivenDBRecordReader<T> {

    private static final Log LOG = LogFactory.getLog(CSVDBRecordReader.class);

    private ResultSet results = null;

    private LongWritable key = null;

    private T value = null;

    private DBInputFormat.DBInputSplit split;

    @SuppressWarnings("unused")
    private Configuration conf;

    private long pos = 0;

    private Connection connection;

    protected PreparedStatement statement;

    @SuppressWarnings("unused")
    private DBConfiguration dbConf;

    @SuppressWarnings("unused")
    private String conditions;

    @SuppressWarnings("unused")
    private String[] fieldNames;

    @SuppressWarnings("unused")
    private Class<T> inputClass;

    @SuppressWarnings("unused")
    private String tableName;

    public static CSVPrinter csvFilePrinter;

    public static Counter ignoreRecordsCounter;

    public static Counter rowErrorCounter;

    public static int MAX_ERROR_LINE;

    public CSVDBRecordReader(DBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn,
            DBConfiguration dbConfig, String cond, String[] fields, String table, String dbProduct) throws SQLException {
        super(split, inputClass, conf, conn, dbConfig, cond, fields, table, dbProduct);
        this.inputClass = inputClass;
        this.split = split;
        this.conf = conf;
        this.connection = conn;
        this.dbConf = dbConfig;
        this.conditions = cond;
        if (fields != null) {
            this.fieldNames = Arrays.copyOf(fields, fields.length);
        }
        this.tableName = table;

    }

    @Override
    public boolean nextKeyValue() throws IOException {
        try {
            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = createValue();
            }
            if (null == this.results) {
                // First time into this method, run the query.
                LOG.info("Working on split: " + split);
                LOG.info("query is: " + getSelectQuery() );
                this.results = executeQuery(getSelectQuery());
            }

            while (true) {
                try {
                    if (!results.next()) {
                        return false;
                    } else {
                        // Set the key field value as the output key value
                        key.set(pos + split.getStart());
                        value.readFields(results);
                        break;
                    }
                } catch (SQLException e) {
                    LOG.info("This row " + (pos + 2) + " is malformed. Skip this row!!");
                    ignoreRecordsCounter.increment(1);
                    rowErrorCounter.increment(1);
                    if (ignoreRecordsCounter.getValue() <= MAX_ERROR_LINE) {
                        csvFilePrinter.printRecord(pos + 2, e.getMessage());
                    }
                } finally {
                    pos++;
                }
            }

        } catch (SQLException e) {
            LoggingUtils.logAll(LOG, e);
            if (this.statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close statement", ex);
                } finally {
                    this.statement = null;
                }
            }
            if (this.connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close connection", ex);
                } finally {
                    this.connection = null;
                }
            }
            if (this.results != null) {
                try {
                    results.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close ResultsSet", ex);
                } finally {
                    this.results = null;
                }
            }

            throw new IOException("SQLException in nextKeyValue", e);
        }
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public T getCurrentValue() {
        return value;
    }
}
