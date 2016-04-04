package com.latticeengines.propdata.yarn.runtime;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatistics;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.aspect.MatchStepAspect;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.MatchPlanner;
import com.latticeengines.propdata.match.service.impl.MatchContext;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("propdataProcessor")
public class PropDataProcessor extends SingleContainerYarnProcessor<PropDataJobConfiguration> {

    private static final Log log = LogFactory.getLog(PropDataProcessor.class);

    @Autowired
    private ApplicationContext appContext;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    @Qualifier("bulkMatchPlanner")
    private MatchPlanner matchPlanner;

    @Autowired
    @Qualifier("bulkMatchExecutor")
    private MatchExecutor matchExecutor;

    @Autowired
    private ColumnMetadataService columnMetadataService;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @Autowired
    private VersionManager versionManager;

    @Autowired
    private Configuration yarnConfiguration;

    private BlockDivider divider;
    private Tenant tenant;
    private ColumnSelection.Predefined predefinedSelection;
    private Map<MatchKey, List<String>> keyMap;
    private Integer blockSize;
    private float progress = 0.07f;
    private UUID rootUid;
    private String avroPath, outputAvro, outputJson;
    private MatchOutput blockOutput;
    private Date receivedAt;
    private Schema schema;

    @Override
    public String process(PropDataJobConfiguration jobConfiguration) throws Exception {
        appContext = loadSoftwarePackages("propdata", softwareLibraryService, appContext, versionManager);
        LogManager.getLogger(MatchStepAspect.class).setLevel(Level.DEBUG);

        receivedAt = new Date();

        String podId = jobConfiguration.getHdfsPodId();
        hdfsPathBuilder.changeHdfsPodId(podId);
        log.info("Use PodId=" + podId);

        String rootOperationUid = jobConfiguration.getRootOperationUid();
        String blockOperationUid = jobConfiguration.getBlockOperationUid();
        rootUid = UUID.fromString(rootOperationUid);
        outputAvro = hdfsPathBuilder.constructMatchBlockDir(rootOperationUid, blockOperationUid)
                .append("block_" + blockOperationUid.replace("-", "_") + ".avro").toString();
        outputJson = hdfsPathBuilder.constructMatchBlockOutputFile(rootOperationUid, blockOperationUid).toString();
        String errFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid).toString();

        try {
            String blockRootDir = hdfsPathBuilder.constructMatchBlockDir(rootOperationUid, blockOperationUid).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, blockRootDir)) {
                HdfsUtils.rmdir(yarnConfiguration, blockRootDir);
            }

            CustomerSpace space = jobConfiguration.getCustomerSpace();
            tenant = new Tenant(space.toString());
            predefinedSelection = jobConfiguration.getPredefinedSelection();
            keyMap = jobConfiguration.getKeyMap();
            blockSize = jobConfiguration.getBlockSize();
            Integer groupSize = jobConfiguration.getGroupSize();

            avroPath = jobConfiguration.getAvroPath();
            divider = new BlockDivider(avroPath, yarnConfiguration, groupSize);
            log.info("Matching a block of " + blockSize + " rows with a group size of " + groupSize);
            schema = constructOutputSchema("PropDataMatchOutput_" + blockOperationUid.replace("-", "_"));
            setProgress(progress);

            while (divider.hasNextGroup()) {
                MatchInput input = constructMatchInputFromData(divider.nextGroup());
                matchBlock(input, 0.9f * input.getNumRows() / blockSize);
                log.info("Processed " + divider.getCount() + " out of " + blockSize + " rows.");
            }

            finalizeBlock();

        } catch (Exception e) {
            String errMessage = String.format("[RootOperationUid=%s BlockOperationUid=%s]\n", rootOperationUid,
                    blockOperationUid);
            errMessage += ExceptionUtils.getFullStackTrace(e);
            HdfsUtils.writeToFile(yarnConfiguration, errFile, errMessage);
            throw(e);
        }
        return null;
    }

    private MatchInput constructMatchInputFromData(List<List<Object>> data) {
        MatchInput matchInput = new MatchInput();
        matchInput.setUuid(rootUid);
        matchInput.setTenant(tenant);
        matchInput.setPredefinedSelection(predefinedSelection);
        matchInput.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
        matchInput.setFields(divider.getFields());
        matchInput.setKeyMap(keyMap);
        matchInput.setData(data);
        return matchInput;
    }

    @MatchStep
    private void matchBlock(MatchInput input, float progressForBlock) {
        MatchContext matchContext = matchPlanner.plan(input);
        progress += progressForBlock * 0.1f;
        setProgress(progress);

        matchContext = matchExecutor.execute(matchContext);
        progress += progressForBlock * 0.8f;

        processMatchOutput(matchContext.getOutput());
        progress += progressForBlock * 0.1f;

        setProgress(progress);
    }

    @MatchStep
    private Schema constructOutputSchema(String recordName) {
        Schema outputSchema = columnMetadataService.getAvroSchema(predefinedSelection, recordName);
        Schema inputSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        inputSchema = prefixFieldName(inputSchema, "Source_");
        return (Schema) AvroUtils.combineSchemas(inputSchema, outputSchema)[0];
    }

    @MatchStep
    private void processMatchOutput(MatchOutput groupOutput) {
        try {
            writeDataToAvro(groupOutput.getResult());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write result to avro.", e);
        }

        MatchOutput newOutput = JsonUtils.deserialize(JsonUtils.serialize(groupOutput), MatchOutput.class);
        List<OutputRecord> cleanedResults = new ArrayList<>();
        for (OutputRecord record : newOutput.getResult()) {
            if (record.getErrorMessages() != null && !record.getErrorMessages().isEmpty()) {
                record.setOutput(null);
                cleanedResults.add(record);
            }
        }
        groupOutput.setResult(cleanedResults);

        if (blockOutput == null) {
            blockOutput = groupOutput;
        } else {
            blockOutput = mergeOutputs(blockOutput, groupOutput);
        }

        log.info("Merge group output into block output.");
    }

    @MatchStep
    private void finalizeBlock() throws IOException {
        finalizeMatchOutput();
        Long count = AvroUtils.count(yarnConfiguration, outputAvro);
        log.info("There are in total " + count + " records in the avro " + outputAvro);
        if (!blockOutput.getStatistics().getRowsMatched().equals(count.intValue())) {
            throw new RuntimeException(
                    String.format("RowsMatched in MatchStatistics [%d] does not equal to the count of the avro [%d].",
                            blockOutput.getStatistics().getRowsMatched(), count));
        }
    }

    private Schema prefixFieldName(Schema schema, String prefix) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(schema.getName());
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder;
        for (Schema.Field field : schema.getFields()) {
            fieldBuilder = fieldAssembler.name(prefix + field.name());
            @SuppressWarnings("deprecation")
            Map<String, String> props = field.props();
            for (Map.Entry<String, String> entry : props.entrySet()) {
                fieldBuilder = fieldBuilder.prop(entry.getKey(), entry.getValue());
            }
            fieldAssembler = AvroUtils.constructFieldWithType(fieldAssembler, fieldBuilder, AvroUtils.getType(field));
        }
        return fieldAssembler.endRecord();
    }

    private void writeDataToAvro(List<OutputRecord> outputRecords) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        for (OutputRecord outputRecord : outputRecords) {
            if (!outputRecord.isMatched() || outputRecord.getOutput() == null || outputRecord.getOutput().isEmpty()) {
                continue;
            }

            List<Object> allValues = new ArrayList<>(outputRecord.getInput());
            allValues.addAll(outputRecord.getOutput());

            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            List<Schema.Field> fields = schema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                Object value = allValues.get(i);
                if (value instanceof Date) {
                    value = ((Date) value).getTime();
                }
                if (value instanceof Timestamp) {
                    value = ((Timestamp) value).getTime();
                }
                builder.set(fields.get(i), value);
            }
            records.add(builder.build());
        }
        if (!HdfsUtils.fileExists(yarnConfiguration, outputAvro)) {
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, outputAvro, records);
        } else {
            AvroUtils.appendToHdfsFile(yarnConfiguration, outputAvro, records);
        }
        log.info("Write " + records.size() + " generic records to " + outputAvro);
    }

    private MatchOutput mergeOutputs(MatchOutput output, MatchOutput newOutput) {
        output.setStatistics(mergeStatistics(output.getStatistics(), newOutput.getStatistics()));
        output.getResult().addAll(newOutput.getResult());
        return output;
    }

    private MatchStatistics mergeStatistics(MatchStatistics stats, MatchStatistics newStats) {
        MatchStatistics mergedStats = new MatchStatistics();
        List<Integer> columnCounts = new ArrayList<>();
        for (int i = 0; i < stats.getColumnMatchCount().size(); i++) {
            columnCounts.add(stats.getColumnMatchCount().get(i) + newStats.getColumnMatchCount().get(i));
        }
        mergedStats.setColumnMatchCount(columnCounts);
        mergedStats.setRowsMatched(stats.getRowsMatched() + newStats.getRowsMatched());
        return mergedStats;
    }

    private void finalizeMatchOutput() throws IOException {
        Date finishedAt = new Date();
        blockOutput.setFinishedAt(finishedAt);
        blockOutput.setReceivedAt(receivedAt);
        blockOutput.getStatistics().setRowsRequested(blockSize);
        blockOutput.getStatistics().setTimeElapsedInMsec(finishedAt.getTime() - receivedAt.getTime());
        matchExecutor.appendMetadata(blockOutput, predefinedSelection);
        HdfsUtils.writeToFile(yarnConfiguration, outputJson, JsonUtils.serialize(blockOutput));
    }

}
