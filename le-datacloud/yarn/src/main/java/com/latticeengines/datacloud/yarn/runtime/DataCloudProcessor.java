package com.latticeengines.datacloud.yarn.runtime;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.aspect.MatchStepAspect;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.metric.MatchResponse;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.impl.BeanDispatcherImpl;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("dataCloudProcessor")
public class DataCloudProcessor extends SingleContainerYarnProcessor<DataCloudJobConfiguration> {

    private static final Log log = LogFactory.getLog(DataCloudProcessor.class);

    private static final String INTEGER = Integer.class.getSimpleName();
    private static final String LONG = Long.class.getSimpleName();
    private static final String FLOAT = Float.class.getSimpleName();
    private static final String DOUBLE = Double.class.getSimpleName();

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
    private SoftwareLibraryService softwareLibraryService;

    @Autowired
    private VersionManager versionManager;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    private ColumnMetadataService columnMetadataService;

    @Autowired
    @Qualifier("accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> columnService;

    @Autowired
    private MetricService metricService;

    private BlockDivider divider;
    private Tenant tenant;
    private Predefined predefinedSelection;
    private String predefinedSelectionVersion;
    private ColumnSelection customizedSelection;
    private Map<MatchKey, List<String>> keyMap;
    private Integer blockSize;
    private String rootOperationUid;
    private String blockOperationUid;
    private String avroPath, outputAvro, outputJson;
    private MatchOutput blockOutput;
    private Date receivedAt;
    private Schema outputSchema;
    private Schema inputSchema;
    private Integer numThreads;
    private MatchInput matchInput;
    private String podId;
    private String dataCloudVersion;
    private Boolean returnUnmatched;
    private Boolean excludePublicDomains;
    private ConcurrentSkipListSet<String> fieldsWithNoMetadata = new ConcurrentSkipListSet<>();
    private Map<String, AccountMasterColumn> accountMasterColumnMap = null;

    @Override
    public String process(DataCloudJobConfiguration jobConfiguration) throws Exception {
        try {
            appContext = loadSoftwarePackages("propdata", softwareLibraryService, appContext, versionManager);
            LogManager.getLogger(MatchStepAspect.class).setLevel(Level.DEBUG);

            dataCloudVersion = jobConfiguration.getDataCloudVersion();
            columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);

            receivedAt = new Date();

            podId = jobConfiguration.getHdfsPodId();
            returnUnmatched = jobConfiguration.getReturnUnmatched();
            excludePublicDomains = jobConfiguration.getExcludePublicDomains();
            HdfsPodContext.changeHdfsPodId(podId);
            log.info("Use PodId=" + podId);

            rootOperationUid = jobConfiguration.getRootOperationUid();
            blockOperationUid = jobConfiguration.getBlockOperationUid();
            outputAvro = hdfsPathBuilder.constructMatchBlockAvro(rootOperationUid, blockOperationUid).toString();
            outputJson = hdfsPathBuilder.constructMatchBlockOutputFile(rootOperationUid, blockOperationUid).toString();

            String blockRootDir = hdfsPathBuilder.constructMatchBlockDir(rootOperationUid, blockOperationUid)
                    .toString();
            if (HdfsUtils.fileExists(yarnConfiguration, blockRootDir)) {
                HdfsUtils.rmdir(yarnConfiguration, blockRootDir);
            }

            CustomerSpace space = jobConfiguration.getCustomerSpace();
            tenant = new Tenant(space.toString());

            predefinedSelection = jobConfiguration.getPredefinedSelection();
            predefinedSelectionVersion = jobConfiguration.getPredefinedSelectionVersion();
            customizedSelection = jobConfiguration.getCustomizedSelection();

            keyMap = jobConfiguration.getKeyMap();
            blockSize = jobConfiguration.getBlockSize();
            Integer groupSize = jobConfiguration.getGroupSize();
            numThreads = jobConfiguration.getThreadPoolSize();
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            avroPath = jobConfiguration.getAvroPath();
            divider = new BlockDivider(avroPath, yarnConfiguration, groupSize);
            log.info("Matching a block of " + blockSize + " rows with a group size of " + groupSize);
            inputSchema = jobConfiguration.getInputAvroSchema();
            outputSchema = constructOutputSchema("PropDataMatchOutput_" + blockOperationUid.replace("-", "_"),
                    jobConfiguration.getDataCloudVersion());

            Integer rowsProcessed = 0;
            setProgress(0.07f);

            List<MatchInput> matchInputs = getInputs();
            while (!matchInputs.isEmpty()) {
                log.info("Processing " + matchInputs.size() + " groups concurrently.");
                List<Future<MatchContext>> futures = new ArrayList<>();
                for (MatchInput matchInput : matchInputs) {
                    Future<MatchContext> future = executor.submit(new MatchCallable(matchInput));
                    futures.add(future);
                }
                for (Future<MatchContext> future : futures) {
                    MatchContext matchContext = future.get();
                    processMatchOutput(matchContext.getOutput());
                    rowsProcessed += matchContext.getInput().getNumRows();
                    setProgress(0.07f + 0.9f * rowsProcessed / blockSize);
                    log.info("Processed " + rowsProcessed + " out of " + blockSize + " rows.");
                }
                matchInputs = getInputs();
            }

            finalizeBlock();
        } catch (Exception e) {
            String rootOperationUid = jobConfiguration.getRootOperationUid();
            String blockOperationUid = jobConfiguration.getBlockOperationUid();
            String errFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid)
                    .toString();
            try {
                HdfsUtils.writeToFile(yarnConfiguration, errFile, ExceptionUtils.getFullStackTrace(e));
            } catch (Exception e1) {
                log.error("Failed to write error to err file.", e1);
            }
            throw (e);
        }

        return null;
    }

    private List<MatchInput> getInputs() {
        List<MatchInput> matchInputs = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            if (divider.hasNextGroup()) {
                MatchInput input = constructMatchInputFromData(divider.nextGroup());
                matchInputs.add(input);
                // cache an input to generate output metric
                this.matchInput = input;
            } else {
                break;
            }
        }
        return matchInputs;
    }

    private MatchInput constructMatchInputFromData(List<List<Object>> data) {
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(rootOperationUid);
        matchInput.setReturnUnmatched(returnUnmatched);
        matchInput.setTenant(tenant);
        matchInput.setPredefinedSelection(predefinedSelection);
        matchInput.setPredefinedVersion(predefinedSelectionVersion);
        matchInput.setCustomSelection(customizedSelection);
        matchInput.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
        matchInput.setFields(divider.getFields());
        matchInput.setKeyMap(keyMap);
        matchInput.setData(data);
        matchInput.setExcludePublicDomains(excludePublicDomains);
        matchInput.setDataCloudVersion(dataCloudVersion);
        return matchInput;
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
        blockOutput = MatchUtils.mergeOutputs(blockOutput, groupOutput);
        log.info("Merge group output into block output.");
    }

    private void writeDataToAvro(List<OutputRecord> outputRecords) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        for (OutputRecord outputRecord : outputRecords) {
            if (!(returnUnmatched || outputRecord.isMatched()) || outputRecord.getOutput() == null
                    || outputRecord.getOutput().isEmpty()) {
                continue;
            }

            List<Object> allValues = new ArrayList<>(outputRecord.getInput());
            allValues.addAll(outputRecord.getOutput());

            GenericRecordBuilder builder = new GenericRecordBuilder(outputSchema);
            List<Schema.Field> fields = outputSchema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                Object value = allValues.get(i);
                if (value instanceof Date) {
                    value = ((Date) value).getTime();
                }
                if (value instanceof Timestamp) {
                    value = ((Timestamp) value).getTime();
                }
                if (MatchTypeUtil.isValidForAccountMasterBasedMatch(dataCloudVersion)) {
                    value = matchDeclaredType(value, fields.get(i).name().replace("Source_", ""));
                }
                builder.set(fields.get(i), value);
            }
            records.add(builder.build());
        }
        if (!HdfsUtils.fileExists(yarnConfiguration, outputAvro)) {
            AvroUtils.writeToHdfsFile(yarnConfiguration, outputSchema, outputAvro, records);
        } else {
            AvroUtils.appendToHdfsFile(yarnConfiguration, outputAvro, records);
        }
        log.info("Write " + records.size() + " generic records to " + outputAvro);
    }

    private Object matchDeclaredType(Object value, String columnId) {
        if (value == null || fieldsWithNoMetadata.contains(columnId)) {
            return value;
        }

        if (accountMasterColumnMap == null) {
            loadAccountMasterColumnMap();
        }

        AccountMasterColumn metadataColumn = accountMasterColumnMap.get(columnId);
        if (metadataColumn == null) {
            fieldsWithNoMetadata.add(columnId);
            return value;
        }
        String javaClass = metadataColumn.getJavaClass();
        if (INTEGER.equalsIgnoreCase(javaClass) && !(value instanceof Integer)) {
            return Integer.valueOf(String.valueOf(value));
        }
        if (LONG.equalsIgnoreCase(javaClass) && !(value instanceof Long)) {
            return Long.valueOf(String.valueOf(value));
        }
        if (FLOAT.equalsIgnoreCase(javaClass) && !(value instanceof Float)) {
            return Float.valueOf(String.valueOf(value));
        }
        if (DOUBLE.equalsIgnoreCase(javaClass) && !(value instanceof Double)) {
            return Double.valueOf(String.valueOf(value));
        }
        return value;
    }

    private void loadAccountMasterColumnMap() {
        accountMasterColumnMap = new HashMap<>();
        List<AccountMasterColumn> amColumns = columnService.scan(dataCloudVersion);
        for (AccountMasterColumn column: amColumns) {
            accountMasterColumnMap.put(column.getColumnId(), column);
        }
    }

    @MatchStep
    private Schema constructOutputSchema(String recordName, String dataCloudVersion) {
        Schema outputSchema = columnMetadataService.getAvroSchema(predefinedSelection, recordName, dataCloudVersion);
        if (inputSchema == null) {
            inputSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
            log.info("Using extracted input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        } else {
            log.info("Using provited input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        }
        inputSchema = prefixFieldName(inputSchema, "Source_");
        return (Schema) AvroUtils.combineSchemas(inputSchema, outputSchema)[0];
    }

    @MatchStep
    private void finalizeBlock() throws IOException {
        finalizeMatchOutput();
        generateOutputMetric(matchInput, blockOutput);
        Long count = AvroUtils.count(yarnConfiguration, outputAvro);
        log.info("There are in total " + count + " records in the avro " + outputAvro);
        if (returnUnmatched) {
            if (!excludePublicDomains && !blockSize.equals(count.intValue())) {
                throw new RuntimeException(String.format(
                        "Block size [%d] does not equal to the count of the avro [%d].", blockSize, count));
            }
        } else {
            // check matched rows
            if (!blockOutput.getStatistics().getRowsMatched().equals(count.intValue())) {
                throw new RuntimeException(String.format(
                        "RowsMatched in MatchStatistics [%d] does not equal to the count of the avro [%d].",
                        blockOutput.getStatistics().getRowsMatched(), count));
            }
        }
        setProgress(1f);
    }

    @MatchStep
    private void generateOutputMetric(MatchInput input, MatchOutput output) {
        try {
            MatchContext context = new MatchContext();
            context.setInput(input);
            context.setOutput(output);
            context.setMatchEngine(MatchContext.MatchEngine.BULK);
            MatchResponse response = new MatchResponse(context);
            metricService.write(MetricDB.LDC_Match, response);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.", e);
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

    private void finalizeMatchOutput() throws IOException {
        Date finishedAt = new Date();
        blockOutput.setFinishedAt(finishedAt);
        blockOutput.setReceivedAt(receivedAt);
        blockOutput.getStatistics().setRowsRequested(blockSize);
        blockOutput.getStatistics().setTimeElapsedInMsec(finishedAt.getTime() - receivedAt.getTime());
        HdfsUtils.writeToFile(yarnConfiguration, outputJson, JsonUtils.serialize(blockOutput));
    }

    private class MatchCallable implements Callable<MatchContext> {

        private MatchInput matchInput;

        MatchCallable(MatchInput matchInput) {
            this.matchInput = matchInput;
        }

        @Override
        public MatchContext call() {
            HdfsPodContext.changeHdfsPodId(podId);
            try {
                Thread.sleep(new Random().nextInt(1500));
            } catch (InterruptedException e) {
                // ignore
            }
            return matchBlock(matchInput);
        }

        @MatchStep
        private MatchContext matchBlock(MatchInput input) {
            MatchContext matchContext = matchPlanner.plan(input);
            return matchExecutor.execute(matchContext);
        }

    }

}
