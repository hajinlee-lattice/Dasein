package com.latticeengines.eai.file.runtime.mapreduce;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.eai.runtime.mapreduce.AvroExportMapper;
import com.latticeengines.eai.runtime.mapreduce.AvroRowHandler;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;

public class CSVExportMapper extends AvroExportMapper implements AvroRowHandler {

    private static final Logger log = LoggerFactory.getLogger(CSVExportMapper.class);

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
        boolean exportUsingDisplayName = config.getBoolean("eai.export.displayname", true);
        List<String> headers = new ArrayList<>();
        for (Field field : schema.getFields()) {
            if (outputField(field)) {
                String header = "";
                if (exportUsingDisplayName) {
                    header = table.getAttribute(field.name()).getDisplayName();
                    if (headers.contains(header)) {
                        header += "_" + System.currentTimeMillis();
                    }
                } else {
                    header = field.name();
                }
                headers.add(header);
            }
        }
        csvFilePrinter = new CSVPrinter(new FileWriter(OUTPUT_FILE),
                LECSVFormat.format.withHeader(headers.toArray(new String[] {})));
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
        } else if (field.name() != null) {
            log.info("Ignore field:" + field.name() + ", value:" + record.get(field.name()));
        }
    }

    @Override
    public void endRecord(Record record) throws IOException {
        log.info(record.toString());
        csvFilePrinter.println();
    }

    private static boolean outputField(Field field) {
        return field.name() != null //
                && !field.name().equals(InterfaceName.InternalId.toString()) //
                && !field.name().equals(ScoreResultField.RawScore.displayName) //
                && !field.name().endsWith(INT_LDC_DEDUPE_ID);
    }
}
