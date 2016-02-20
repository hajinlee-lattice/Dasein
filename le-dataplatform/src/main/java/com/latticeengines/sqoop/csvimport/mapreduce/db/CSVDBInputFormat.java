package com.latticeengines.sqoop.csvimport.mapreduce.db;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.sqoop.mapreduce.DBWritable;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

@SuppressWarnings({ "deprecation" })
public class CSVDBInputFormat<T extends DBWritable> extends DataDrivenDBInputFormat<T> {

    private static final Log LOG = LogFactory.getLog(CSVDBInputFormat.class);

    public static void setInput(Job job, Class<? extends DBWritable> inputClass, String tableName, String conditions,
            String splitBy, String... fieldNames) {
        DBInputFormat.setInput(job, inputClass, tableName, conditions, splitBy, fieldNames);
        job.setInputFormatClass(CSVDBInputFormat.class);
    }

    /**
     * setInput() takes a custom query and a separate "bounding query" to use
     * instead of the custom "count query" used by DBInputFormat.
     */
    public static void setInput(Job job, Class<? extends DBWritable> inputClass, String inputQuery,
            String inputBoundingQuery) {
        DBInputFormat.setInput(job, inputClass, inputQuery, "");
        job.getConfiguration().set(DBConfiguration.INPUT_BOUNDING_QUERY, inputBoundingQuery);
        job.setInputFormatClass(CSVDBInputFormat.class);
    }

    @Override
    protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split, Configuration conf)
            throws IOException {

        DBConfiguration dbConf = getDBConf();
        @SuppressWarnings("unchecked")
        Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
        String dbProductName = getDBProductName();

        LOG.debug("Creating db record reader for db product: " + dbProductName);

        try {
            return new CSVDBRecordReader<T>(split, inputClass, conf, getConnection(), dbConf,
                    dbConf.getInputConditions(), dbConf.getInputFieldNames(), dbConf.getInputTableName(), dbProductName);
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
    }
}
