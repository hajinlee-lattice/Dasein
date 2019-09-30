package com.latticeengines.eai.file.runtime.mapreduce;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
    private Set<String> exclusionColumns = new HashSet<>();
    private Set<String> inclusionColumns = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileSplit split = (FileSplit) context.getInputSplit();
        splitName = StringUtils.substringBeforeLast(split.getPath().getName(), ".");
    }

    @Override
    protected AvroRowHandler initialize(
            Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable>.Context context, Schema schema)
            throws IOException {
        table = JsonUtils.deserialize(config.get("eai.table.schema"), Table.class);
        boolean exportUsingDisplayName = config.getBoolean("eai.export.displayname", true);
        getExclusionColumns();
        getInclusionColumns();
        List<String> headers = new ArrayList<>();
        for (Field field : schema.getFields()) {
            if (outputField(field)) {
                String header = "";
                if (exportUsingDisplayName) {
                    String displayName = table.getAttribute(field.name()).getDisplayName();
                    header = displayName;
                    if (headers.contains(header)) {
                        header = displayName + "_" + field.name();
                        if (headers.contains(header)) {
                            header = displayName + "_" + System.currentTimeMillis();
                            if (headers.contains(header)) {
                                throw new LedpException(LedpCode.LEDP_18109,
                                        new String[] { displayName + " has conflicts with naming convention." });
                            }
                        }
                    }
                } else {
                    header = field.name();
                }
                headers.add(header);
            } else {
                log.info("Ignore field:" + field.name());
            }
        }
        csvFilePrinter = new CSVPrinter(new BufferedWriter(new FileWriter(OUTPUT_FILE), 8192 * 2),
                LECSVFormat.format.withHeader(headers.toArray(new String[] {})));
        return this;
    }

    private void getInclusionColumns() {
        String columns = config.get(ExportProperty.EXPORT_INCLUSION_COLUMNS);
        if (columns == null || columns.length() == 0) {
            return;
        }
        String[] tokens = columns.split(";");
        inclusionColumns.addAll(Arrays.asList(tokens));
    }

    private void getExclusionColumns() {
        String columns = config.get(ExportProperty.EXPORT_EXCLUSION_COLUMNS);
        if (columns == null || columns.length() == 0) {
            return;
        }
        String[] tokens = columns.split(";");
        exclusionColumns.addAll(Arrays.asList(tokens));
    }

    @Override
    protected void finalize(Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable>.Context context)
            throws IOException {
        Configuration config = getConfig();
        csvFilePrinter.flush();
        String outputFileName = context.getConfiguration().get(MapReduceProperty.OUTPUT.name());
        HdfsUtils.mkdir(config, new Path(outputFileName).getParent().toString());
        HdfsUtils.copyLocalToHdfs(config, OUTPUT_FILE, outputFileName + "_" + splitName + ".csv");
        csvFilePrinter.close();
    }

    @Override
    public void startRecord(Record record) {
    }

    @Override
    public void handleField(Record record, Field field) throws IOException {
        if (outputField(field)) {
            String fieldValue = String.valueOf(record.get(field.name()));
            Attribute attr = table.getAttribute(field.name());
            if (fieldValue.equals("null")) {
                fieldValue = "";
            } else if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Date)) {
                Class<?> javaType = AvroUtils.getJavaType(AvroUtils.getType(field));
                if (Long.class.equals(javaType) || Integer.class.equals(javaType)) {
                    fieldValue = TimeStampConvertUtils.convertToDate(Long.valueOf(fieldValue));
                }
            }
            csvFilePrinter.print(fieldValue);
        } else if (field.name() != null) {
            if (log.isTraceEnabled()) {
                log.trace("Ignore field:" + field.name() + ", value:" + record.get(field.name()));
            }
        }
    }

    @Override
    public void endRecord(Record record) throws IOException {
        csvFilePrinter.println();
    }

    private boolean outputField(Field field) {
        return field.name() != null //
                && !field.name().equals(InterfaceName.InternalId.toString()) //
                && !field.name().equals(ScoreResultField.RawScore.displayName) //
                && !field.name().endsWith(INT_LDC_DEDUPE_ID) //
                && !exclusionColumns.contains(field.name()) //
                && (CollectionUtils.isNotEmpty(inclusionColumns) ? inclusionColumns.contains(field.name()) : true);
    }
}
