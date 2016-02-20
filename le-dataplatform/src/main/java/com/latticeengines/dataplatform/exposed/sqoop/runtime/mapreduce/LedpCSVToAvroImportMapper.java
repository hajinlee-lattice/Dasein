package com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.sqoop.mapreduce.AvroJob;
import org.mortbay.log.Log;

import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.sqoop.csvimport.mapreduce.db.CSVDBRecordReader;

/**
 * Imports records by transforming them to Avro records in an Avro data file.
 */
@SuppressWarnings("deprecation")
public class LedpCSVToAvroImportMapper extends
        AutoProgressMapper<LongWritable, SqoopRecord, AvroWrapper<GenericRecord>, NullWritable> {
    private final AvroWrapper<GenericRecord> wrapper = new AvroWrapper<GenericRecord>();
    private Schema schema;
    private Table table;
    private LargeObjectLoader lobLoader;
    private Path outputPath;
    private CSVPrinter csvFilePrinter;
    private Map<String, String> errorMap;
    private String interpretation;
    private boolean emailOrWebsiteIsEmpty;
    
    private static final String ERROR_FILE = "error.csv";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        schema = AvroJob.getMapOutputSchema(conf);
        table = JsonUtils.deserialize(context.getConfiguration().get("lattice.eai.file.schema"), Table.class);
        interpretation = table.getInterpretation();
        lobLoader = new LargeObjectLoader(conf, FileOutputFormat.getWorkOutputPath(context));

        outputPath = AvroOutputFormat.getOutputPath(new JobConf(context.getConfiguration()));
        Log.info("Path is:" + outputPath);
        csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE), CSVFormat.RFC4180.withHeader("LineNumber",
                "ErrorMessage").withDelimiter(','));
        CSVDBRecordReader.csvFilePrinter = csvFilePrinter;
        CSVDBRecordReader.ignoreRecordsCounter = context.getCounter(RecordImportCounter.IGNORED_RECORDS);
        errorMap = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, SqoopRecord val, Context context) throws IOException, InterruptedException {
        try {
            Log.info("Using LedpCSVToAvroImportMapper");
            // Loading of LOBs was delayed until we have a Context.
            val.loadLargeObjects(lobLoader);
        } catch (SQLException sqlE) {
            throw new IOException(sqlE);
        }

        emailOrWebsiteIsEmpty = false;
        GenericRecord record = toGenericRecord(val);
        if (errorMap.size() == 0) {
            wrapper.datum(record);
            context.write(wrapper, NullWritable.get());
            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).increment(1);
        } else {
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).increment(1);
            long lineNum = context.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue()
                    + context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue();
            csvFilePrinter.printRecord(lineNum + 1, errorMap.toString());
            csvFilePrinter.flush();
            errorMap.clear();
        }
    }

    private GenericRecord toGenericRecord(SqoopRecord val) {
        GenericRecord record = new GenericData.Record(schema);
        Map<String, Object> fieldMap = val.getFieldMap();
        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            String fieldKey = entry.getKey();
            String fieldCsvValue = String.valueOf(entry.getValue());
            Object fieldAvroValue = null;
            Type avroType = schema.getField(fieldKey).schema().getTypes().get(0).getType();
            try {
                validateRowValueBeforeConvertToAvro(interpretation, fieldKey, fieldCsvValue);
                LOG.info("Validation Passed! Starting to convert to avro value.");
                fieldAvroValue = toAvro(fieldCsvValue, avroType,
                        table.getNameAttributeMap().get(fieldKey));
            } catch (Exception e) {
                LOG.error(e);
                errorMap.put(fieldKey, e.getMessage());
            }
            record.put(fieldKey, fieldAvroValue);
        }
        return record;
    }

    private void validateRowValueBeforeConvertToAvro(String interpretation, String fieldKey, String fieldCsvValue) {
        if((fieldKey.equals("Id") || fieldKey.equals("IsWon")) && StringUtils.isEmpty(fieldCsvValue)){
            throw new RuntimeException(String.format("Required Column %s is missing value", fieldKey));
        }
        else if(interpretation.equals(SchemaInterpretation.SalesforceAccount.name())){
            if(fieldKey.equals("Website") && StringUtils.isEmpty(fieldCsvValue)){
                emailOrWebsiteIsEmpty = true;
            }
            else if(emailOrWebsiteIsEmpty && (fieldKey.equals("Name") || fieldKey.equals("BillingCity") || fieldKey.equals("BillingState") || fieldKey.equals("BillingCountry")) && StringUtils.isEmpty(fieldCsvValue)){
                throw new RuntimeException(String.format("Website is empty, so %s cannot be empty", fieldKey));
            }
        }
        else if(interpretation.equals(SchemaInterpretation.SalesforceLead.name())){
            if(fieldKey.equals("Email") && StringUtils.isEmpty(fieldCsvValue)){
                emailOrWebsiteIsEmpty = true;
            }
            else if(emailOrWebsiteIsEmpty &&(fieldKey.equals("Company") || fieldKey.equals("City") || fieldKey.equals("State") || fieldKey.equals("Country")) && StringUtils.isEmpty(fieldCsvValue)){
                throw new RuntimeException(String.format("Email is empty, so %s cannot be empty", fieldKey));
            }
        }
    }

    private Object toAvro(String fieldCsvValue, Type avroType, Attribute attr) {
        try{
            switch (avroType) {
            case DOUBLE:
                return Double.valueOf(fieldCsvValue);
            case FLOAT:
                return Float.valueOf(fieldCsvValue);
            case INT:
                return Integer.valueOf(fieldCsvValue);
            case LONG:
                if (attr.getLogicalDataType().equals("Date") || attr.getLogicalDataType().equals("Timestamp")) {
                    DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
                    Log.info(fieldCsvValue);
                    Log.info("parse :" + df.parse(fieldCsvValue));
                    return df.parse(fieldCsvValue).getTime();
                } else {
                    return Long.valueOf(fieldCsvValue);
                }
            case STRING:
                return fieldCsvValue;
            case BOOLEAN:
                return Boolean.valueOf(fieldCsvValue);
            case ENUM:
                return fieldCsvValue;
            default:
                throw new RuntimeException("Not supported Field, avroType:" + avroType + ", logicalType:"
                        + attr.getLogicalDataType());
            }
        } catch(NumberFormatException e){
            throw new RuntimeException("Cannot convert " + fieldCsvValue + " to " + avroType);
        } catch (ParseException e) {
            throw new RuntimeException("Cannot parse " + fieldCsvValue + " as Date or Timestamp using MM-dd-yyyy");
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

    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException {
        // String s = "abc";
        // DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
        // System.out.println(df.parse(s));
        // Class.forName("org.relique.jdbc.csv.CsvDriver");
        // Properties props = new Properties();
        // Define column names and column data types here.
        // Class klass = CsvDriver.class;
        // URL location = klass.getResource('/'+klass.getName().replace('.',
        // '/')+".class");
        // System.out.println(location);
        // props.put(CsvDriver.MISSING_VALUE, "$$");
        // props.put(CsvDriver.IGNORE_UNPARSEABLE_LINES, "String,String");
        // Connection conn = DriverManager.getConnection("jdbc:relique:csv:" +
        // "", props);
        // ResultSet rs =
        // conn.prepareStatement("Select * from fil1.csv").getResultSet();
        // System.out.println(rs);
    }

}
