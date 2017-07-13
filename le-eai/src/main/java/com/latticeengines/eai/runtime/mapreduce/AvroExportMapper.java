package com.latticeengines.eai.runtime.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.mapreduce.counters.RecordExportCounter;

public abstract class AvroExportMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(AvroExportMapper.class);

    private Schema schema;

    private AvroRowHandler avroRowHandler;

    protected Configuration config;

    protected abstract AvroRowHandler initialize(Context context, Schema schema)
            throws IOException, InterruptedException;

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
        log.info(schema.toString());
    }

    @Override
    public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException {
        try {
            Record record = key.datum();
            avroRowHandler.startRecord(record);
            for (Field field : schema.getFields()) {
                avroRowHandler.handleField(record, field);
            }
            avroRowHandler.endRecord(record);
        } catch (Exception e) {
            context.getCounter(RecordExportCounter.ERROR_RECORDS).increment(1);
            throw e;
        }
        context.getCounter(RecordExportCounter.EXPORTED_RECORDS).increment(1);
        if (context.getCounter(RecordExportCounter.EXPORTED_RECORDS).getValue() % 100000 == 0) {
            log.info("Exported " + context.getCounter(RecordExportCounter.EXPORTED_RECORDS).getValue() + " records.");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        finalize(context);
        if (context.getCounter(RecordExportCounter.EXPORTED_RECORDS).getValue() == 0) {
            context.getCounter(RecordExportCounter.EXPORTED_RECORDS).setValue(0);
        }
        if (context.getCounter(RecordExportCounter.ERROR_RECORDS).getValue() == 0) {
            context.getCounter(RecordExportCounter.ERROR_RECORDS).setValue(0);
        }
        if (context.getCounter(RecordExportCounter.SCANNED_RECORDS).getValue() == 0) {
            context.getCounter(RecordExportCounter.SCANNED_RECORDS).setValue(0);
        }
    }

}
