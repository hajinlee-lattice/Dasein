package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;

public class CSVExportMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(CSVExportMapper.class);

    private static final String OUTPUT_FILE = "output.csv";

    private CSVPrinter csvFilePrinter;

    private Schema schema;

    private Configuration config;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        config = context.getConfiguration();
        schema = AvroJob.getInputKeySchema(config);
        List<String> headers = new ArrayList<>();
        for (Field field : schema.getFields()) {
            headers.add(field.name());
        }

        csvFilePrinter = new CSVPrinter(new FileWriter(OUTPUT_FILE), CSVFormat.RFC4180.withDelimiter(',').withHeader(
                headers.toArray(new String[] {})));

    }

    @Override
    public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException {
        Record record = key.datum();
        for (Field field : schema.getFields()) {
            String fieldValue = String.valueOf(record.get(field.name()));
            if (fieldValue == null) {
                fieldValue = "";
            }
            csvFilePrinter.print(fieldValue);
        }
        csvFilePrinter.println();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        csvFilePrinter.flush();
        String outputFileName = context.getConfiguration().get(MapReduceProperty.OUTPUT.name());
        HdfsUtils.mkdir(config, new Path(outputFileName).getParent().toString());
        HdfsUtils.copyLocalToHdfs(config, OUTPUT_FILE, outputFileName);
        csvFilePrinter.close();
    }
}
