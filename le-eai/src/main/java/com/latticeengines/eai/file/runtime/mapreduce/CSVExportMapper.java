package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.runtime.mapreduce.AvroExportMapper;
import com.latticeengines.eai.runtime.mapreduce.AvroRowHandler;

public class CSVExportMapper extends AvroExportMapper implements AvroRowHandler {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(CSVExportMapper.class);

    private static final String OUTPUT_FILE = "output.csv";

    private CSVPrinter csvFilePrinter;

    private String splitName;

    private Table table;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileSplit split = (FileSplit) context.getInputSplit();
        splitName = StringUtils.substringBeforeLast(split.getPath().getName(), ".");
    }

    @Override
    protected AvroRowHandler initialize(
            Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable>.Context context, Schema schema)
            throws IOException, InterruptedException {
        table = JsonUtils.deserialize(config.get("eai.table.schema"), Table.class);
        System.out.println(table);
        List<String> headers = new ArrayList<>();
        for (Field field : schema.getFields()) {
            if (outputField(field)) {
                headers.add(table.getAttribute(field.name()).getDisplayName());
            }
        }
        csvFilePrinter = new CSVPrinter(new FileWriter(OUTPUT_FILE), CSVFormat.RFC4180.withDelimiter(',').withHeader(
                headers.toArray(new String[] {})));
        return this;
    }

    @Override
    protected void finalize(Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Configuration config = getConfig();
        csvFilePrinter.flush();
        String outputFileName = context.getConfiguration().get(MapReduceProperty.OUTPUT.name());
        HdfsUtils.mkdir(config, new Path(outputFileName).getParent().toString());
        HdfsUtils.copyLocalToHdfs(config, OUTPUT_FILE, outputFileName + "_" + splitName + ".csv");
        csvFilePrinter.close();
    }

    @Override
    public void startRecord(Record record) throws IOException {
    }

    @Override
    public void handleField(Record record, Field field) throws IOException {
        if (outputField(field)) {
            String fieldValue = String.valueOf(record.get(field.name()));
            Attribute attr = table.getAttribute(field.name());
            if (fieldValue.equals("null")) {
                fieldValue = "";
            } else if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Date)) {
                fieldValue = TimeStampConvertUtils.convertToDate(Long.valueOf(fieldValue));
            }
            csvFilePrinter.print(fieldValue);
        }
    }

    @Override
    public void endRecord(Record record) throws IOException {
        csvFilePrinter.println();
    }

    private static boolean outputField(Field field) {
        return field.name() != null && !field.name().equals(InterfaceName.InternalId.toString());
    }

}
