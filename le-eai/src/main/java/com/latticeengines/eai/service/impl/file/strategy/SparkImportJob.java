//package com.latticeengines.eai.service.impl.file.strategy;
//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.PrintWriter;
//import java.io.Serializable;
//import java.text.ParseException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Locale;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.avro.Schema;
//import org.apache.avro.Schema.Type;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.mapred.AvroKey;
//import org.apache.avro.mapreduce.AvroJob;
//import org.apache.avro.mapreduce.AvroKeyOutputFormat;
//import org.apache.commons.collections.CollectionUtils;
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVParser;
//import org.apache.commons.csv.CSVPrinter;
//import org.apache.commons.csv.CSVRecord;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.util.LongAccumulator;
//import org.spark_project.guava.collect.ImmutableMap;
//import org.springframework.format.number.NumberFormatter;
//
//import com.google.common.annotations.VisibleForTesting;
//import com.latticeengines.common.exposed.csv.LECSVFormat;
//import com.latticeengines.common.exposed.util.HdfsUtils;
//import com.latticeengines.common.exposed.util.JsonUtils;
//import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
//import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
//import com.latticeengines.domain.exposed.metadata.Attribute;
//import com.latticeengines.domain.exposed.metadata.InterfaceName;
//import com.latticeengines.domain.exposed.metadata.LogicalDataType;
//import com.latticeengines.domain.exposed.metadata.Table;
//import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
//import com.latticeengines.domain.exposed.util.TableUtils;
//
//import scala.Tuple2;
//
//public class SparkImportJob implements Serializable {
//
//    private static final long serialVersionUID = -5341285729956512136L;
//
//    private static Configuration yarnConfiguration = new Configuration();
//
//    public static final String IMPORTED_COUNT = "IMPORTED_COUNT";
//
//    public static final String IGNORED_COUNT = "IGNORED_COUNT";
//
//    public static final String MISSING_REQUIRED_FIELD_COUNT = "MISSING_REQUIRED_FIELD_COUNT";
//
//    public static final String FIELD_MALEFORMED_COUNT = "FIELD_MALEFORMED_COUNT";
//
//    public static final String ROW_ERROR_COUNT = "ROW_ERROR_COUNT";
//
//    public JavaSparkContext createSparkContext() {
//        SparkConf conf = new SparkConf().setAppName("csvImport")//
//                .set("spark.hadoop.yarn.timeline-service.enabled", "false")
//                .set("spark.eventLog.dir", "hdfs://localhost:9000/shared/spark-logs")
//                .set("spark.eventLog.enabled", "true")
//                .set("spark.executor.heartbeatInterval", "200s");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.hadoopConfiguration().addResource(yarnConfiguration);
//        return sc;
//    }
//
//    @SuppressWarnings("serial")
//    public void importData(JavaSparkContext sc) throws Exception {
//
//        Properties props = JsonUtils.deserialize(sc.getConf().get("spark.props"), Properties.class);
//        String outputPath = props.getProperty(MapReduceProperty.OUTPUT.name());
//
//        LongAccumulator importedCount = sc.sc().longAccumulator(IMPORTED_COUNT);
//        LongAccumulator ignoredCount = sc.sc().longAccumulator(IGNORED_COUNT);
//        LongAccumulator missingRequiredFieldCount = sc.sc().longAccumulator(MISSING_REQUIRED_FIELD_COUNT);
//        LongAccumulator fieldMaledFormedCount = sc.sc().longAccumulator(FIELD_MALEFORMED_COUNT);
//        LongAccumulator rowErrorCount = sc.sc().longAccumulator(ROW_ERROR_COUNT);
//
//        Map<String, LongAccumulator> accumulatorMap = ImmutableMap.<String, LongAccumulator> of(IMPORTED_COUNT,
//                importedCount, IGNORED_COUNT, ignoredCount, MISSING_REQUIRED_FIELD_COUNT, missingRequiredFieldCount,
//                FIELD_MALEFORMED_COUNT, fieldMaledFormedCount, ROW_ERROR_COUNT, rowErrorCount);
//
//        Table table = sc.broadcast(JsonUtils.deserialize(props.getProperty("eai.table.schema"), Table.class)).value();
//
//        Schema schema = TableUtils.createSchema(table.getName(), table);
//        Job job = Job.getInstance(yarnConfiguration);
//        AvroJob.setOutputKeySchema(job, schema);
//
//        JavaRDD<String> lines = sc.textFile(props.getProperty(MapReduceProperty.INPUT.name()));
//
//        lines.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, AvroKey<GenericRecord>, NullWritable>() {
//
//            Map<ImportError, List<String>> errors = new HashMap<>();
//
//            String errorPath = outputPath + "/error.csv";
//
//            @Override
//            public Iterator<Tuple2<AvroKey<GenericRecord>, NullWritable>> call(Iterator<String> iter) throws Exception {
//                List<Tuple2<AvroKey<GenericRecord>, NullWritable>> tuples = new ArrayList<>();
//                CSVFormat format = LECSVFormat.format;
//                if (!HdfsUtils.fileExists(yarnConfiguration, errorPath)) {
//                    format = format.withHeader("LineNumber", "Id", "ErrorMessage");
//                } else {
//                    format = format.withSkipHeaderRecord();
//                }
//                String[] headers;
//                try (CSVParser parser = new CSVParser(new InputStreamReader(
//                        HdfsUtils.getInputStream(yarnConfiguration, props.getProperty(MapReduceProperty.INPUT.name()))),
//                        LECSVFormat.format)) {
//                    headers = new ArrayList<String>(parser.getHeaderMap().keySet()).toArray(new String[] {});
//                }
//                try (CSVPrinter csvFilePrinter = new CSVPrinter(
//                        new PrintWriter(HdfsUtils.getAppendableOutputStream(yarnConfiguration, errorPath)), format)) {
//                    while (iter.hasNext()) {
//                        List<CSVRecord> records = CSVParser.parse(iter.next(), LECSVFormat.format.withHeader(headers))
//                                .getRecords();
//                        if (records.isEmpty()) {
//                            System.out.println("empty!");
//                            continue;
//                        }
//                        initErrorsMap(errors);
//                        CSVRecord csvRecord = records.get(0);
//                        GenericRecord avroRecord = toGenericRecord(csvRecord, table, errors);
//                        if (!processErrors(accumulatorMap, errors)) {
//                            tuples.add(new Tuple2<AvroKey<GenericRecord>, NullWritable>(
//                                    new AvroKey<GenericRecord>(avroRecord), NullWritable.get()));
//                        } else {
//                            String id = table.getAttribute(InterfaceName.Id) == null ? ""
//                                    : csvRecord.get(table.getAttribute(InterfaceName.Id).getDisplayName());
//                            handleErrors(csvRecord.getRecordNumber(), id, csvFilePrinter);
//                        }
//                    }
//                }
//                return tuples.iterator();
//            }
//
//            private void initErrorsMap(Map<ImportError, List<String>> errors) {
//                errors.clear();
//                errors.put(ImportError.MISSING_REQUIRED_FIELD, new ArrayList<>());
//                errors.put(ImportError.FIELD_MALEFORMED, new ArrayList<>());
//                errors.put(ImportError.ROW_ERROR, new ArrayList<>());
//            }
//
//            private boolean processErrors(Map<String, LongAccumulator> accumulatorMap,
//                    Map<ImportError, List<String>> errors) {
//                boolean hasError = false;
//                if (!errors.get(ImportError.MISSING_REQUIRED_FIELD).isEmpty()) {
//                    accumulatorMap.get(MISSING_REQUIRED_FIELD_COUNT).add(1);
//                    hasError = true;
//                }
//                if (!errors.get(ImportError.FIELD_MALEFORMED).isEmpty()) {
//                    accumulatorMap.get(FIELD_MALEFORMED_COUNT).add(1);
//                    hasError = true;
//                }
//                if (!errors.get(ImportError.ROW_ERROR).isEmpty()) {
//                    accumulatorMap.get(ROW_ERROR_COUNT).add(1);
//                    hasError = true;
//                }
//                if (hasError) {
//                    accumulatorMap.get(IGNORED_COUNT).add(1);
//                }
//                return hasError;
//            }
//
//            private void handleErrors(long lineNumber, String id, CSVPrinter csvFilePrinter) {
//                try {
//                    csvFilePrinter.printRecord(lineNumber, id, errors.values().toString());
//                    csvFilePrinter.flush();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//            }
//
//        }).saveAsNewAPIHadoopFile(outputPath, AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class,
//                job.getConfiguration());
//
//        // try (CSVParser parser = new CSVParser(new InputStreamReader(new
//        // FileInputStream("outputRvDomainsvLeftv2.csv")),
//        // LECSVFormat.format)) {
//        //
//        // Broadcast<List<CSVRecord>> broadCastCSVRecords =
//        // sc.broadcast(parser.getRecords());
//        //
//        // sc.parallelize(broadCastCSVRecords.value()).mapPartitionsToPair(
//        // new PairFlatMapFunction<Iterator<CSVRecord>, AvroKey<GenericRecord>,
//        // NullWritable>() {
//        //
//        // Map<ImportError, List<String>> errors = new HashMap<>();
//        //
//        // String errorPath = outputPath + "/error.csv";
//        //
//        // @Override
//        // public Iterator<Tuple2<AvroKey<GenericRecord>, NullWritable>>
//        // call(Iterator<CSVRecord> iter)
//        // throws Exception {
//        // List<Tuple2<AvroKey<GenericRecord>, NullWritable>> tuples = new
//        // ArrayList<>();
//        // CSVFormat format = LECSVFormat.format;
//        // if (!HdfsUtils.fileExists(yarnConfiguration, errorPath)) {
//        // format = format.withHeader("LineNumber", "Id", "ErrorMessage");
//        // } else {
//        // format = format.withSkipHeaderRecord();
//        // }
//        // try (CSVPrinter csvFilePrinter = new CSVPrinter(
//        // new
//        // PrintWriter(HdfsUtils.getAppendableOutputStream(yarnConfiguration,
//        // errorPath)),
//        // format)) {
//        // while (iter.hasNext()) {
//        // initErrorsMap(errors);
//        // CSVRecord csvRecord = iter.next();
//        // GenericRecord avroRecord = toGenericRecord(csvRecord, table, errors);
//        // System.out.println(errors);
//        // if (!processErrors(accumulatorMap, errors)) {
//        // tuples.add(new Tuple2<AvroKey<GenericRecord>, NullWritable>(
//        // new AvroKey<GenericRecord>(avroRecord), NullWritable.get()));
//        // } else {
//        // String id = table.getAttribute(InterfaceName.Id) == null ? ""
//        // :
//        // csvRecord.get(table.getAttribute(InterfaceName.Id).getDisplayName());
//        // handleErrors(csvRecord.getRecordNumber(), id, csvFilePrinter);
//        // }
//        // }
//        // }
//        // return tuples.iterator();
//        // }
//        //
//        // private void initErrorsMap(Map<ImportError, List<String>> errors) {
//        // errors.clear();
//        // errors.put(ImportError.MISSING_REQUIRED_FIELD, new ArrayList<>());
//        // errors.put(ImportError.FIELD_MALEFORMED, new ArrayList<>());
//        // errors.put(ImportError.ROW_ERROR, new ArrayList<>());
//        // }
//        //
//        // private boolean processErrors(Map<String, LongAccumulator>
//        // accumulatorMap,
//        // Map<ImportError, List<String>> errors) {
//        // boolean hasError = false;
//        // if (!errors.get(ImportError.MISSING_REQUIRED_FIELD).isEmpty()) {
//        // accumulatorMap.get(MISSING_REQUIRED_FIELD_COUNT).add(1);
//        // hasError = true;
//        // }
//        // if (!errors.get(ImportError.FIELD_MALEFORMED).isEmpty()) {
//        // accumulatorMap.get(FIELD_MALEFORMED_COUNT).add(1);
//        // hasError = true;
//        // }
//        // if (!errors.get(ImportError.ROW_ERROR).isEmpty()) {
//        // accumulatorMap.get(ROW_ERROR_COUNT).add(1);
//        // hasError = true;
//        // }
//        // if (hasError) {
//        // accumulatorMap.get(IGNORED_COUNT).add(1);
//        // }
//        // System.out.println("HasError");
//        // return hasError;
//        // }
//        //
//        // private void handleErrors(long lineNumber, String id, CSVPrinter
//        // csvFilePrinter) {
//        // try {
//        // csvFilePrinter.printRecord(lineNumber, id,
//        // errors.values().toString());
//        // csvFilePrinter.flush();
//        // } catch (IOException e) {
//        // e.printStackTrace();
//        // }
//        //
//        // }
//        // }).saveAsNewAPIHadoopFile(outputPath, AvroKey.class,
//        // NullWritable.class, AvroKeyOutputFormat.class,
//        // job.getConfiguration());
//        //
//        // }
//    }
//
//    private GenericRecord toGenericRecord(CSVRecord csvRecord, Table table, Map<ImportError, List<String>> errors) {
//        GenericRecord avroRecord = new GenericData.Record(TableUtils.createSchema(table.getName(), table));
//        for (Attribute attr : table.getAttributes()) {
//            Object avroFieldValue = null;
//            String csvFieldValue = null;
//            if (csvRecord.isMapped(attr.getDisplayName())) {
//                try {
//                    csvFieldValue = String.valueOf(csvRecord.get(attr.getDisplayName()));
//                } catch (Exception e) { // This catch is for the row error
//                    e.printStackTrace();
//                }
//                List<InputValidator> validators = attr.getValidators();
//                if (validateAttribute(validators, csvRecord, attr, table, errors)) {
//                    if (!attr.isNullable() || !StringUtils.isEmpty(csvFieldValue)) {
//                        Type avroType = Type.valueOf(attr.getPhysicalDataType().toUpperCase());
//                        avroFieldValue = toAvro(csvFieldValue, avroType, attr, errors);
//                    }
//                    avroRecord.put(attr.getName(), avroFieldValue);
//                }
//            }
//        }
//        avroRecord.put(InterfaceName.InternalId.name(), csvRecord.getRecordNumber());
//        return avroRecord;
//    }
//
//    private boolean validateAttribute(List<InputValidator> validators, CSVRecord csvRecord, Attribute attr, Table table,
//            Map<ImportError, List<String>> errors) {
//        String attrKey = attr.getName();
//        if (!attr.isNullable() && StringUtils.isEmpty(csvRecord.get(attr.getDisplayName()))) {
//            errors.get(ImportError.MISSING_REQUIRED_FIELD)
//                    .add(String.format("Required Column %s is missing value.", attr.getDisplayName()));
//            return false;
//        }
//        if (CollectionUtils.isNotEmpty(validators)) {
//            InputValidator validator = validators.get(0);
//            try {
//                validator.validate(attrKey, csvRecord.toMap(), table);
//            } catch (Exception e) {
//                errors.get(ImportError.MISSING_REQUIRED_FIELD).add(e.getMessage());
//                return false;
//            }
//        }
//        return true;
//    }
//
//    private Object toAvro(String fieldCsvValue, Type avroType, Attribute attr, Map<ImportError, List<String>> errors) {
//        try {
//            switch (avroType) {
//            case DOUBLE:
//                return new Double(parseStringToNumber(fieldCsvValue).doubleValue());
//            case FLOAT:
//                return new Float(parseStringToNumber(fieldCsvValue).floatValue());
//            case INT:
//                return new Integer(parseStringToNumber(fieldCsvValue).intValue());
//            case LONG:
//                if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Date)) {
//                    return TimeStampConvertUtils.convertToLong(fieldCsvValue);
//                } else {
//                    return new Long(parseStringToNumber(fieldCsvValue).longValue());
//                }
//            case STRING:
//                return fieldCsvValue;
//            case ENUM:
//                return fieldCsvValue;
//            case BOOLEAN:
//                if (fieldCsvValue.equals("1") || fieldCsvValue.equalsIgnoreCase("true")) {
//                    return Boolean.TRUE;
//                } else if (fieldCsvValue.equals("0") || fieldCsvValue.equalsIgnoreCase("false")) {
//                    return Boolean.FALSE;
//                }
//                throw new IllegalArgumentException();
//            default:
//                errors.get(ImportError.ROW_ERROR).add("Not supported Field, avroType: " + avroType
//                        + ", physicalDatalType:" + attr.getPhysicalDataType());
//            }
//        } catch (IllegalArgumentException e) {
//            errors.get(ImportError.FIELD_MALEFORMED).add(String.format("Cannot convert %s to type %s for column %s.",
//                    fieldCsvValue, avroType, attr.getDisplayName()));
//        } catch (Exception e) {
//            errors.get(ImportError.FIELD_MALEFORMED).add(String.format(
//                    "Cannot parse %s as Date or Timestamp for column %s.", fieldCsvValue, attr.getDisplayName()));
//        }
//        return null;
//    }
//
//    @VisibleForTesting
//    Number parseStringToNumber(String inputStr) throws ParseException {
//        NumberFormatter numberFormatter = new NumberFormatter();
//        return numberFormatter.parse(inputStr, Locale.getDefault());
//    }
//
//    public static void main(String[] args) throws Exception {
//        SparkImportJob job = new SparkImportJob();
//        try (JavaSparkContext sc = job.createSparkContext()) {
//            job.importData(sc);
//        }
//    }
//
//    public enum ImportError {
//
//        MISSING_REQUIRED_FIELD, //
//        FIELD_MALEFORMED, //
//        ROW_ERROR;
//
//    }
//}
