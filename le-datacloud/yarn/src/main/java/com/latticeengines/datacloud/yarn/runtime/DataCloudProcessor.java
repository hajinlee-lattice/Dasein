package com.latticeengines.datacloud.yarn.runtime;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.aspect.MatchStepAspect;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.metric.MatchResponse;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.impl.BeanDispatcherImpl;
import com.latticeengines.datacloud.match.service.impl.MatchConstants;
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
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("dataCloudProcessor")
public class DataCloudProcessor extends SingleContainerYarnProcessor<DataCloudJobConfiguration> {

    private static final Log log = LogFactory.getLog(DataCloudProcessor.class);

    private static final String INTEGER = Integer.class.getSimpleName();
    private static final String LONG = Long.class.getSimpleName();
    private static final String FLOAT = Float.class.getSimpleName();
    private static final String DOUBLE = Double.class.getSimpleName();
    private static final String STRING = String.class.getSimpleName();
    private static final String MATCHOUTPUT_BUFFER_FILE = "matchoutput.buffer";
    private static final Long TIME_OUT_PER_10K = TimeUnit.MINUTES.toMillis(20);

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

    @Autowired
    private MatchActorSystem matchActorSystem;

    private ColumnMetadataService columnMetadataService;

    @Autowired
    @Qualifier("accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> columnService;

    @Autowired
    private MetricService metricService;

    @Value("${datacloud.match.num.threads}")
    private Integer sqlThreadPool;

    @Value("${datacloud.match.bulk.group.size}")
    private Integer sqlGroupSize;

    @Value("${datacloud.yarn.num.travelers}")
    private int numTravelers;

    @Autowired
    private MatchProxy matchProxy;

    private BlockDivider divider;
    private Tenant tenant;
    private Predefined predefinedSelection;
    private String predefinedSelectionVersion;
    private ColumnSelection customizedSelection;
    private Map<MatchKey, List<String>> keyMap;
    private Integer blockSize;
    private String rootOperationUid;
    private String avroPath, outputAvro, outputJson;
    private MatchOutput blockOutput;
    private Date receivedAt;
    private Schema outputSchema;
    private Schema inputSchema;
    private MatchInput matchInput;
    private String podId;
    private String dataCloudVersion;
    private Boolean returnUnmatched;
    private Boolean excludeUnmatchedWithPublicDomain;
    private Boolean publicDomainAsNormalDomain;
    private ConcurrentSkipListSet<String> fieldsWithNoMetadata = new ConcurrentSkipListSet<>();
    private Map<String, AccountMasterColumn> accountMasterColumnMap = null;
    private boolean useProxy = false;

    private AtomicInteger rowsProcessed = new AtomicInteger(0);

    @Override
    public String process(DataCloudJobConfiguration jobConfiguration) throws Exception {
        try {
            appContext = loadSoftwarePackages("propdata", softwareLibraryService, appContext, versionManager);
            LogManager.getLogger(MatchStepAspect.class).setLevel(Level.DEBUG);

            matchActorSystem.setBatchMode(true);

            dataCloudVersion = jobConfiguration.getDataCloudVersion();
            columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);

            receivedAt = new Date();

            podId = jobConfiguration.getHdfsPodId();
            returnUnmatched = true;
            excludeUnmatchedWithPublicDomain = jobConfiguration.getExcludeUnmatchedPublicDomain();
            publicDomainAsNormalDomain = jobConfiguration.getPublicDomainAsNormalDomain();
            HdfsPodContext.changeHdfsPodId(podId);
            log.info("Use PodId=" + podId);

            rootOperationUid = jobConfiguration.getRootOperationUid();
            String blockOperationUid = jobConfiguration.getBlockOperationUid();
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
            predefinedSelectionVersion = "1.0";
            customizedSelection = jobConfiguration.getCustomizedSelection();

            keyMap = jobConfiguration.getKeyMap();
            blockSize = jobConfiguration.getBlockSize();
            Long timeOut = Math.max(Math.round(TIME_OUT_PER_10K * blockSize / 10000.0), TimeUnit.MINUTES.toMillis(10));
            log.info(String.format("Set timeout to be %.2f for %d records", (timeOut / 60000.0), blockSize));

            useProxy = Boolean.TRUE.equals(jobConfiguration.getUseRealTimeProxy());
            if (useProxy) {
                String overwritingProxyUrl = jobConfiguration.getRealTimeProxyUrl();
                if (StringUtils.isNotEmpty(overwritingProxyUrl)) {
                    matchProxy.setHostport(overwritingProxyUrl);
                }
                log.info("Using real-time match proxy at " + matchProxy.getHostport());
            }

            Integer groupSize = jobConfiguration.getGroupSize();
            if (MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion)) {
                // for actor system bulk match, match records one by one
                groupSize = 1;
            } else {
                if (groupSize == null || groupSize < 1) {
                    groupSize = sqlGroupSize;
                }
            }

            Integer numThreads = jobConfiguration.getThreadPoolSize();
            if (MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion) && !useProxy) {
                numThreads = numTravelers;
            } else {
                if (numThreads == null || numThreads < 1) {
                    numThreads = sqlThreadPool;
                }
            }
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            avroPath = jobConfiguration.getAvroPath();
            divider = new BlockDivider(avroPath, yarnConfiguration, groupSize);
            log.info("Matching a block of " + blockSize + " rows with a group size of " + groupSize
                    + " and a thread pool of size " + numThreads);
            inputSchema = jobConfiguration.getInputAvroSchema();
            outputSchema = constructOutputSchema("PropDataMatchOutput_" + blockOperationUid.replace("-", "_"),
                    jobConfiguration.getDataCloudVersion());

            setProgress(0.07f);
            Long startTime = System.currentTimeMillis();
            Set<Future<MatchContext>> futures = new HashSet<>();
            this.matchInput = null;
            while (divider.hasNextGroup()) {
                if (futures.size() < numTravelers) {
                    // create new input object for each record
                    MatchInput input = constructMatchInputFromData(divider.nextGroup());
                    // cache an input to generate output metric
                    if (this.matchInput == null) {
                        this.matchInput = JsonUtils.deserialize(JsonUtils.serialize(input), MatchInput.class);
                    }

                    Future<MatchContext> future;
                    if (useProxy) {
                        future = executor.submit(new RealTimeMatchCallable(input));
                    } else {
                        future = executor.submit(new BulkMatchCallable(input));
                    }
                    futures.add(future);
                }
                if (futures.size() >= numTravelers) {
                    consumeFutures(futures);
                }
            }

            while (!futures.isEmpty()) {
                consumeFutures(futures);
                if (System.currentTimeMillis() - startTime > timeOut) {
                    throw new RuntimeException(String.format("Did not finish matching %d rows in %.2f minutes.",
                            blockSize, timeOut / 60000.0));
                }
            }

            log.info(String.format("Finished matching %d rows in %.2f minutes.", blockSize,
                    (System.currentTimeMillis() - startTime) / 60000.0));
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

    private void consumeFutures(Collection<Future<MatchContext>> futures) {
        List<Future<MatchContext>> toDelete = new ArrayList<>();

        MatchContext combinedContext = null;
        for (Future<MatchContext> future : futures) {
            MatchContext context;
            try {
                context = future.get(100, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                continue;
            }

            // always skip this future if it has not timed out.
            toDelete.add(future);

            if (context != null) {
                if (combinedContext == null) {
                    combinedContext = context;
                } else {
                    combinedContext.getOutput().getResult().addAll(context.getOutput().getResult());
                }
                rowsProcessed.addAndGet(context.getInput().getNumRows());
            }
        }
        if (combinedContext != null && !combinedContext.getOutput().getResult().isEmpty()) {
            processMatchOutput(combinedContext.getOutput());
            int rows = rowsProcessed.get();
            setProgress(0.07f + 0.9f * rows / blockSize);
            log.info("Processed " + rows + " out of " + blockSize + " rows.");
        }

        futures.removeAll(toDelete);
    }

    private MatchInput constructMatchInputFromData(List<List<Object>> data) {
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(rootOperationUid);
        matchInput.setTenant(tenant);
        matchInput.setPredefinedSelection(predefinedSelection);
        matchInput.setPredefinedVersion(predefinedSelectionVersion);
        matchInput.setCustomSelection(customizedSelection);
        matchInput.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
        matchInput.setFields(divider.getFields());
        matchInput.setKeyMap(keyMap);
        matchInput.setData(data);
        matchInput.setExcludeUnmatchedWithPublicDomain(excludeUnmatchedWithPublicDomain);
        matchInput.setPublicDomainAsNormalDomain(publicDomainAsNormalDomain);
        matchInput.setDataCloudVersion(dataCloudVersion);

        if (useProxy) {
            matchInput.setSkipKeyResolution(true);
        }

        return matchInput;
    }

    @MatchStep
    private void processMatchOutput(MatchOutput groupOutput) {
        try {
            writeDataToAvro(groupOutput.getResult());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write result to avro.", e);
        }

        List<OutputRecord> recordsWithErrors = new ArrayList<>();
        // per record error might cause out of memory
        // TODO:: change to stream to output file on the fly
        // for (OutputRecord record : groupOutput.getResult()) {
        // if (record.getErrorMessages() != null &&
        // !record.getErrorMessages().isEmpty()) {
        // record.setOutput(null);
        // recordsWithErrors.add(record);
        // }
        // }
        groupOutput.setResult(recordsWithErrors);

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
                if (MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion)
                        || fields.get(i).name().equalsIgnoreCase(MatchConstants.LID_FIELD)) {
                    if (fields.get(i).name().startsWith("Source_")) {
                        value = matchDeclaredType(value, fields.get(i).name().replace("Source_", ""));
                    } else {
                        value = matchDeclaredType(value, fields.get(i).name());
                    }
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
        if (STRING.equalsIgnoreCase(javaClass) && !(value instanceof String)) {
            return String.valueOf(value);
        }
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
        for (AccountMasterColumn column : amColumns) {
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
            log.info("Using provided input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        }
        inputSchema = prefixFieldName(inputSchema, outputSchema, "Source_");
        return (Schema) AvroUtils.combineSchemas(inputSchema, outputSchema)[0];
    }

    @MatchStep
    private void finalizeBlock() throws IOException {
        finalizeMatchOutput();
        generateOutputMetric(matchInput, blockOutput);
        Long count = AvroUtils.count(yarnConfiguration, outputAvro);
        log.info("There are in total " + count + " records in the avro " + outputAvro);
        if (returnUnmatched) {
            if (!excludeUnmatchedWithPublicDomain && !blockSize.equals(count.intValue())) {
                throw new RuntimeException(String
                        .format("Block size [%d] does not equal to the count of the avro [%d].", blockSize, count));
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

    private Schema prefixFieldName(Schema schema, Schema offendingSchema, String prefix) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(schema.getName());
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder;
        Set<String> offendingFields = new HashSet<>();
        for (Schema.Field field : offendingSchema.getFields()) {
            offendingFields.add(field.name().toUpperCase());
        }
        for (Schema.Field field : schema.getFields()) {
            if (offendingFields.contains(field.name().toUpperCase())) {
                // name conflict
                fieldBuilder = fieldAssembler.name(prefix + field.name());
            } else {
                fieldBuilder = fieldAssembler.name(field.name());
            }
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
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(new File(MATCHOUTPUT_BUFFER_FILE), blockOutput);
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, MATCHOUTPUT_BUFFER_FILE, outputJson);
            FileUtils.deleteQuietly(new File(MATCHOUTPUT_BUFFER_FILE));
        } catch (Exception e) {
            log.error("Failed to save match output json.", e);
        }
    }

    private class BulkMatchCallable implements Callable<MatchContext> {

        private MatchInput matchInput;

        BulkMatchCallable(MatchInput matchInput) {
            this.matchInput = matchInput;
        }

        @Override
        public MatchContext call() {
            HdfsPodContext.changeHdfsPodId(podId);
            try {
                Thread.sleep(new Random().nextInt(200));
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

    private class RealTimeMatchCallable implements Callable<MatchContext> {

        private MatchInput matchInput;

        RealTimeMatchCallable(MatchInput matchInput) {
            this.matchInput = matchInput;
        }

        @Override
        public MatchContext call() {
            HdfsPodContext.changeHdfsPodId(podId);
            return matchBlock(matchInput);
        }

        @MatchStep
        private MatchContext matchBlock(MatchInput input) {
            MatchContext matchContext = new MatchContext();
            matchContext.setInput(input);
            MatchOutput output = matchProxy.matchRealTime(input);
            matchContext.setOutput(output);
            return matchContext;
        }

    }

}
