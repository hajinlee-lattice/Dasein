package com.latticeengines.eai.runtime.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class AvroExportMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AvroExportMapper.class);

    private Schema schema;

    private Configuration config;
    
    private AvroRowHandler avroRowHandler;
    
    protected abstract AvroRowHandler initialize(Context context, Schema schema) throws IOException, InterruptedException;
    
    protected abstract void finalize(Context context) throws IOException, InterruptedException;
    
    public AvroExportMapper() {
    }
    
    protected void setAvroRowHandler(AvroRowHandler avroRowHandler) {
        this.avroRowHandler = avroRowHandler;
    }
    
    protected Configuration getConfig() {
        return config;
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        config = context.getConfiguration();
        schema = AvroJob.getInputKeySchema(config);
        avroRowHandler = initialize(context, schema);
    }

    @Override
    public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException {
        Record record = key.datum();
        avroRowHandler.startRecord(record);
        for (Field field : schema.getFields()) {
            avroRowHandler.handleField(record, field);
        }
        avroRowHandler.endRecord(record);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        finalize(context);
    }

}
