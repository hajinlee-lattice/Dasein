package com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.mapreduce.AvroImportMapper;
import org.apache.sqoop.mapreduce.AvroJob;
import org.mortbay.log.Log;

import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

/**
 * Imports records by transforming them to Avro records in an Avro data file.
 */
@SuppressWarnings("deprecation")
public class LedpCSVToAvroImportMapper extends AvroImportMapper {
    private final AvroWrapper<GenericRecord> wrapper = new AvroWrapper<GenericRecord>();
    private Schema schema;
    private Table table;
    private LargeObjectLoader lobLoader;
    private Path outputPath;
    private CSVPrinter csvFilePrinter;
    
    private static final String ERROR_FILE = "error.csv";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        schema = AvroJob.getMapOutputSchema(conf);
        table = JsonUtils.deserialize(context.getConfiguration().get("lattice.eai.file.schema"), Table.class);
        lobLoader = new LargeObjectLoader(conf, FileOutputFormat.getWorkOutputPath(context));

        outputPath = AvroOutputFormat.getOutputPath(new JobConf(context.getConfiguration()));
        Log.info("Path is:" + outputPath);
        csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE), CSVFormat.RFC4180.withHeader("LineNumber", "ErrorMessage").withDelimiter(StringUtils.COMMA));
    }

    @Override
    protected void map(LongWritable key, SqoopRecord val, Context context) throws IOException, InterruptedException {
        try {
            Log.info("Using LedpCSVToAvroImportMapper");
            Log.info("Table is: " + table);
            // Loading of LOBs was delayed until we have a Context.
            val.loadLargeObjects(lobLoader);
        } catch (SQLException sqlE) {
            throw new IOException(sqlE);
        }

        GenericRecord record = new GenericData.Record(schema);
        Map<String, Object> fieldMap = val.getFieldMap();
        ObjectNode obj = new ObjectMapper().createObjectNode();
        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            String recordKey = null;
            Object recordValue = null;
            Type avroType = null;
            try {
                recordKey = entry.getKey();
                avroType = schema.getField(recordKey).schema().getTypes().get(0).getType();
                recordValue = toAvro2(String.valueOf(entry.getValue()), avroType, table.getNameAttributeMap().get(recordKey));
                record.put(recordKey, recordValue);
            } catch (Exception e) {
                obj.put(recordKey, "Cannot convert " + entry.getValue() + " to " + avroType);
            }
        }
        if(obj.size() == 0){
            wrapper.datum(record);
            context.write(wrapper, NullWritable.get());
            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).increment(1);
        }else{
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).increment(1);
            long lineNum = context.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue() + context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue();
            csvFilePrinter.printRecord(lineNum, obj.toString());
            csvFilePrinter.flush();
        }
    }

    private Object toAvro2(String s, Type avroType, Attribute attr) {
        switch (avroType) {
        case DOUBLE:
            return Double.valueOf(s);
        case FLOAT:
            return Float.valueOf(s);
        case INT:
            return Integer.valueOf(s);
        case LONG:
            if (attr.getLogicalDataType().equals("Date") || attr.getLogicalDataType().equals("Timestamp")) {
                DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
                try {
                    Log.info(s);
                    Log.info("parse :" + df.parse(s));
                    return df.parse(s).getTime();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                return Long.valueOf(s);
            }
        case STRING:
            return s;
        case BOOLEAN:
            return Boolean.valueOf(s);
        case ENUM:
            return s;
        default:
            throw new RuntimeException("Not supported Field, avroType:" + avroType + ", logicalType:"
                    + attr.getLogicalDataType());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException {
        if (null != lobLoader) {
            lobLoader.close();
        }
        csvFilePrinter.close();
        HdfsUtils.copyLocalToHdfs(context.getConfiguration(), ERROR_FILE, outputPath + "/" + ERROR_FILE);
        if (context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() == 0) {
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).setValue(0);
        }
    }

    public static void main(String[] args) throws ParseException {
        String s = "2001-07-04T12:08:56.235-0700";
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        System.out.println(df.parse(s));

    }
}
