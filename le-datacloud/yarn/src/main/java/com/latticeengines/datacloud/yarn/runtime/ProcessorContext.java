package com.latticeengines.datacloud.yarn.runtime;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_ID_FIELD;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_MATCH_ERROR_FIELD;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_NAME_FIELD;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_REMOVED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.impl.BeanDispatcherImpl;
import com.latticeengines.datacloud.match.service.impl.MatchPlannerBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

@Component("processorContext")
public class ProcessorContext {

    private static final Logger log = LoggerFactory.getLogger(ProcessorContext.class);

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    @Autowired
    private MatchProxy matchProxy;

    @Resource(name = "bulkMatchPlanner")
    private MatchPlanner matchPlanner;

    @Value("${datacloud.match.num.threads}")
    private Integer sqlThreadPool;

    @Value("${datacloud.match.bulk.group.size}")
    private Integer sqlGroupSize;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    @Value("${datacloud.yarn.actors.num.threads}")
    private int actorsThreadPool;

    @Value("${datacloud.yarn.fetchonly.num.threads}")
    private int fetchonlyThreadPool;

    @Value("${datacloud.yarn.actors.group.size}")
    private int actorsGroupSize;

    private static final Long TIME_OUT_PER_10K = TimeUnit.MINUTES.toMillis(20);

    private DataCloudJobConfiguration jobConfiguration;

    private Tenant tenant;
    private Predefined predefinedSelection;
    private ColumnSelection columnSelection;
    private Map<MatchKey, List<String>> keyMap;
    private Integer blockSize;
    private String rootOperationUid, blockOperationUid;
    private String avroPath, outputJson;
    private MatchOutput blockOutput;
    private Date receivedAt;
    private Schema outputSchema;
    private Schema inputSchema;
    private MatchInput groupMatchInput;
    private String podId;
    private String dataCloudVersion;
    private Boolean returnUnmatched;
    private Boolean excludePublicDomain;
    private Boolean publicDomainAsNormalDomain;
    private boolean useProxy = false;
    private String decisionGraph;
    private int splits = 1;
    private boolean datacloudOnly = false;

    private BlockDivider divider;
    private String blockRootDir;
    private Integer numThreads;

    private boolean partialMatch;

    private AtomicInteger rowsProcessed = new AtomicInteger(0);

    private long timeOut;

    private DataCloudProcessor dataCloudProcessor;

    private boolean useRemoteDnB;

    private boolean matchDebugEnabled;

    private long recordTimeOut;

    private boolean disableDunsValidation;

    private MatchInput originalInput;

    private List<ColumnMetadata> metadatas;
    private List<String> metadataFields;

    public DataCloudJobConfiguration getJobConfiguration() {
        return jobConfiguration;
    }

    public DataCloudProcessor getDataCloudProcessor() {
        return dataCloudProcessor;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public Map<MatchKey, List<String>> getKeyMap() {
        return keyMap;
    }

    public Integer getBlockSize() {
        return blockSize;
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public String getBlockOperationUid() {
        return blockOperationUid;
    }

    public String getOutputAvroGlob() {
        return hdfsPathBuilder.constructMatchBlockAvroGlob(rootOperationUid, blockOperationUid);
    }

    public String getOutputAvro(int split) {
        return "output/block_" + AvroUtils.getAvroFriendlyString(blockOperationUid)
                + String.format("-p-%05d.avro", split);
    }

    public String getHdfsOutputDir() {
        return hdfsPathBuilder.constructMatchBlockDir(rootOperationUid, blockOperationUid).toString();
    }

    public String getErrorOutputAvro(int split) {
        return hdfsPathBuilder.constructMatchBlockErrorSplitAvro(rootOperationUid, blockOperationUid, split).toString();
    }

    public String getNewEntityOutputAvro(int split) {
        return hdfsPathBuilder.constructMatchBlockNewEntitySplitAvro(rootOperationUid, blockOperationUid, split)
                .toString();
    }

    public String getOutputJson() {
        return outputJson;
    }

    public MatchOutput getBlockOutput() {
        return blockOutput;
    }

    public void setBlockOutput(MatchOutput blockOutput) {
        this.blockOutput = blockOutput;
    }

    public Date getReceivedAt() {
        return receivedAt;
    }

    public Schema getInputSchema() {
        return inputSchema;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    public MatchInput getGroupMatchInput() {
        return groupMatchInput;
    }

    public void setGroupMatchInput(MatchInput groupMatchInput) {
        this.groupMatchInput = groupMatchInput;
    }

    public String getPodId() {
        return podId;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public Boolean getReturnUnmatched() {
        return returnUnmatched;
    }

    public Boolean getExcludePublicDomain() {
        return excludePublicDomain;
    }

    public Boolean getPublicDomainAsNormalDomain() {
        return publicDomainAsNormalDomain;
    }

    public boolean isUseProxy() {
        return useProxy;
    }

    public String getDecisionGraph() {
        return decisionGraph;
    }

    public AtomicInteger getRowsProcessed() {
        return rowsProcessed;
    }

    public BlockDivider getDivider() {
        return divider;
    }

    public Integer getNumThreads() {
        return numThreads;
    }

    public long getTimeOut() {
        return timeOut;
    }

    public long getRecordTimeOut() {
        return recordTimeOut;
    }

    public boolean isUseRemoteDnB() {
        return useRemoteDnB;
    }

    public boolean isMatchDebugEnabled() {
        return matchDebugEnabled;
    }

    public boolean isDisableDunsValidation() {
        return disableDunsValidation;
    }

    public MatchInput getOriginalInput() {
        return originalInput;
    }

    public void setOriginalInput(MatchInput originalInput) {
        this.originalInput = originalInput;
    }

    public int getSplits() {
        return splits;
    }

    public boolean isPartialMatch() {
        return partialMatch;
    }

    public void setPartialMatch(boolean partialMatch) {
        this.partialMatch = partialMatch;
    }

    public ColumnSelection getColumnSelection() {
        return columnSelection;
    }

    public List<ColumnMetadata> getMetadatas() {
        return metadatas;
    }

    public List<String> getMetadataFields() {
        return metadataFields;
    }

    public void initialize(DataCloudProcessor dataCloudProcessor, DataCloudJobConfiguration jobConfiguration)
            throws Exception {
        this.jobConfiguration = jobConfiguration;
        this.dataCloudProcessor = dataCloudProcessor;
        setMatchInput(jobConfiguration);
        originalInput = jobConfiguration.getMatchInput();

        dataCloudVersion = originalInput.getDataCloudVersion();
        log.info("Use DataCloudVersion=" + dataCloudVersion);

        receivedAt = new Date();

        podId = jobConfiguration.getHdfsPodId();

        returnUnmatched = true;
        excludePublicDomain = originalInput.getExcludePublicDomain();
        publicDomainAsNormalDomain = originalInput.isPublicDomainAsNormalDomain();
        HdfsPodContext.changeHdfsPodId(podId);
        log.info("Use PodId=" + podId);

        rootOperationUid = jobConfiguration.getRootOperationUid();
        blockOperationUid = jobConfiguration.getBlockOperationUid();
        outputJson = hdfsPathBuilder.constructMatchBlockOutputFile(rootOperationUid, blockOperationUid).toString();

        blockRootDir = hdfsPathBuilder.constructMatchBlockDir(rootOperationUid, blockOperationUid).toString();
        if (originalInput.getSplitsPerBlock() != null) {
            splits = Math.max(1, originalInput.getSplitsPerBlock());
            log.info("Generating " + splits + " splits.");
        }

        CustomerSpace space = jobConfiguration.getCustomerSpace();
        tenant = new Tenant(space.toString());
        datacloudOnly = Boolean.TRUE.equals(originalInput.getDataCloudOnly())
                || (!zkConfigurationService.isCDLTenant(space)
                        && !OperationalMode.isEntityMatch(originalInput.getOperationalMode()));

        if (originalInput.getUnionSelection() != null || originalInput.getCustomSelection() != null) {
            if (OperationalMode.ENTITY_MATCH_ATTR_LOOKUP.equals(originalInput.getOperationalMode())) {
                metadatas = ((MatchPlannerBase) matchPlanner).parseCDLMetadata(originalInput);
                columnSelection = new ColumnSelection();
                List<Column> columns = metadatas.stream() //
                        .map(cm -> new Column(cm.getAttrName())) //
                        .collect(Collectors.toList());
                columnSelection.setColumns(columns);
            } else {
                columnSelection = ((MatchPlannerBase) matchPlanner).parseColumnSelection(originalInput);
            }
        } else {
            predefinedSelection = originalInput.getPredefinedSelection();
        }

        if (Boolean.TRUE.equals(originalInput.isFetchOnly())) {
            useRemoteDnB = false;
        } else {
            if (originalInput.getUseRemoteDnB() != null) {
                useRemoteDnB = originalInput.getUseRemoteDnB();
            } else {
                useRemoteDnB = true;
            }
            useRemoteDnB = useRemoteDnB && MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion);
            useRemoteDnB = useRemoteDnB && zkConfigurationService.useRemoteDnBGlobal();
        }
        log.info("Use remote DnB ? " + useRemoteDnB);

        // TODO: Feeling match planning for bulk match is not well-organized.
        // Some of them happen in BulkMatchPlanner, some of them happen here
        ((MatchPlannerBase) matchPlanner).setEntityDecisionGraph(originalInput);
        log.info("Match target entity: " + originalInput.getTargetEntity());
        decisionGraph = originalInput.getDecisionGraph();
        if (StringUtils.isEmpty(decisionGraph)) {
            decisionGraph = defaultGraph;
            log.info("Decision graph is not provided, use default " + decisionGraph);
        } else {
            log.info("Use decision graph " + decisionGraph);
        }

        keyMap = originalInput.getKeyMap();
        blockSize = jobConfiguration.getBlockSize();
        timeOut = Math.max(Math.round(TIME_OUT_PER_10K * blockSize / 10000.0), TimeUnit.MINUTES.toMillis(60));
        this.recordTimeOut = timeOut;
        if (useRemoteDnB) {
            timeOut = timeOut * 2;
        }
        log.info(String.format("Set timeout to be %.2f minutes for %d records", (timeOut / 60000.0), blockSize));

        useProxy = Boolean.TRUE.equals(originalInput.getUseRealTimeProxy());
        if (useProxy) {
            String overwritingProxyUrl = jobConfiguration.getRealTimeProxyUrl();
            if (StringUtils.isNotEmpty(overwritingProxyUrl)) {
                matchProxy.setHostport(overwritingProxyUrl);
            }
            log.info("Using real-time match proxy at " + matchProxy.getHostport());
        }

        Integer groupSize = jobConfiguration.getGroupSize();
        numThreads = jobConfiguration.getThreadPoolSize();
        if (MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion)) {
            groupSize = actorsGroupSize;
            numThreads = actorsThreadPool;
            if (useRemoteDnB) {
                groupSize = 128;
            } else if (originalInput.isFetchOnly()) {
                groupSize = 500;
                numThreads = fetchonlyThreadPool;
            }
        } else {
            if (groupSize == null || groupSize < 1) {
                groupSize = sqlGroupSize;
            }
            if (numThreads == null || numThreads < 1) {
                numThreads = sqlThreadPool;
            }
        }

        avroPath = jobConfiguration.getAvroPath();
        divider = new BlockDivider(avroPath, yarnConfiguration, groupSize);
        log.info("Matching a block of " + blockSize + " rows with a group size of " + groupSize
                + " and a thread pool of size " + numThreads);
        inputSchema = jobConfiguration.getInputAvroSchema();
        outputSchema = constructOutputSchema("DataCloudMatchOutput_" + blockOperationUid.replace("-", "_"),
                originalInput);

        // sequence is very important in outputschema
        // am output attr -> dedupe -> debug
        // the same sequence will be used in writeDataToAvro
        log.info("Need to prepare for dedupe: " + originalInput.isPrepareForDedupe());
        if (MatchRequestSource.MODELING.equals(originalInput.getRequestSource())
                && originalInput.isPrepareForDedupe()) {
            outputSchema = appendDedupeHelpers(outputSchema);
        }

        matchDebugEnabled = zkConfigurationService.isMatchDebugEnabled(space) || originalInput.isMatchDebugEnabled();
        originalInput.setMatchDebugEnabled(matchDebugEnabled);
        log.info("Match Debug Enabled=" + matchDebugEnabled);
        if (matchDebugEnabled) {
            outputSchema = appendDebugSchema(outputSchema);
        }

        disableDunsValidation = originalInput.isDisableDunsValidation();
        log.info(String.format("Duns validation is disabled: %b", disableDunsValidation));
        cleanup();

        log.info("Partial match enabled=" + originalInput.isPartialMatchEnabled() + " partial match=" + partialMatch);
    }

    private void setMatchInput(DataCloudJobConfiguration jobConfiguration) {
        if (jobConfiguration.getMatchInput().getInputBuffer() == null
                && StringUtils.isNotBlank(jobConfiguration.getMatchInputPath())) {
            try {
                String jsonStr = HdfsUtils.getHdfsFileContents(yarnConfiguration, jobConfiguration.getMatchInputPath());
                jobConfiguration.setMatchInput(JsonUtils.deserialize(jsonStr, MatchInput.class));
                log.info("Read MatchInput from hdfs, file=" + jobConfiguration.getMatchInputPath());
            } catch (Exception e) {
                log.warn("Can not read MatchInput from hdfs, file=" + jobConfiguration.getMatchInputPath());
            }
        }

    }

    private Schema appendDebugSchema(Schema schema) {
        Map<String, Class<?>> fieldMap = new LinkedHashMap<>();
        for (String field : MatchConstants.matchDebugFields) {
            fieldMap.put(field, String.class);
        }
        Map<String, Map<String, String>> propertiesMap = getPropertiesMap(fieldMap.keySet());
        Schema debugSchema = AvroUtils.constructSchemaWithProperties(schema.getName(), fieldMap, propertiesMap);
        return (Schema) AvroUtils.combineSchemas(schema, debugSchema)[0];
    }

    Schema appendErrorSchema(Schema schema) {
        Map<String, Class<?>> fieldMap = new LinkedHashMap<>();
        fieldMap.put(ENTITY_MATCH_ERROR_FIELD, String.class);
        Map<String, Map<String, String>> propertiesMap = getPropertiesMap(fieldMap.keySet());
        Schema errorSchema = AvroUtils.constructSchemaWithProperties(schema.getName(), fieldMap, propertiesMap);
        return (Schema) AvroUtils.combineSchemas(schema, errorSchema)[0];
    }

    Schema getNewEntitySchema() {
        Map<String, Class<?>> fieldMap = new LinkedHashMap<>();
        fieldMap.put(ENTITY_NAME_FIELD, String.class);
        fieldMap.put(ENTITY_ID_FIELD, String.class);
        Map<String, Map<String, String>> propertiesMap = getPropertiesMap(fieldMap.keySet());
        String inputSchemaName = getInputSchema().getName();
        return AvroUtils.constructSchemaWithProperties(inputSchemaName, fieldMap, propertiesMap);
    }

    private Map<String, Map<String, String>> getPropertiesMap(Set<String> keySet) {
        Map<String, Map<String, String>> propertiesMap = new HashMap<String, Map<String, String>>();
        Map<String, String> properties = new HashMap<>();
        properties.put("ApprovedUsage", "[None]");
        for (String key : keySet) {
            propertiesMap.put(key, properties);
        }
        return propertiesMap;
    }

    private Schema appendDedupeHelpers(Schema schema) {
        Map<String, Class<?>> fieldMap = new LinkedHashMap<>();
        fieldMap.put(INT_LDC_LID, String.class);
        fieldMap.put(INT_LDC_DEDUPE_ID, String.class);
        fieldMap.put(INT_LDC_REMOVED, Integer.class);

        Map<String, Map<String, String>> propertiesMap = getPropertiesMap(fieldMap.keySet());
        Schema dedupeSchema = AvroUtils.constructSchemaWithProperties(schema.getName(), fieldMap, propertiesMap);
        return (Schema) AvroUtils.combineSchemas(schema, dedupeSchema)[0];
    }

    private void cleanup() throws IOException {
        if (HdfsUtils.fileExists(yarnConfiguration, blockRootDir)) {
            HdfsUtils.rmdir(yarnConfiguration, blockRootDir);
        }
    }

    @MatchStep
    private Schema constructOutputSchema(String recordName, MatchInput input) {
        Schema outputSchema;
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
        if (datacloudOnly) {
            if (columnSelection == null) {
                log.info("Generating output schema using predefined selection " + predefinedSelection);
                outputSchema = columnMetadataService.getAvroSchema(predefinedSelection, recordName, dataCloudVersion);
            } else {
                log.info("Generating output schema using custom/union selection with "
                        + columnSelection.getColumns().size() + " columns");
                metadatas = columnMetadataService.fromSelection(columnSelection, dataCloudVersion);
                outputSchema = columnMetadataService.getAvroSchemaFromColumnMetadatas(metadatas, recordName,
                        dataCloudVersion);
                metadataFields = parseMetadataFields();
            }
            log.info("Output schema has " + outputSchema.getFields().size() + " fields from data cloud.");
        } else if (OperationalMode.isEntityMatch(input.getOperationalMode())) {
            // TODO: Feeling match planning for bulk match is not
            // well-organized. Some of them happen in BulkMatchPlanner, some of
            // them happen here
            if (metadatas == null) {
                if (OperationalMode.ENTITY_MATCH_ATTR_LOOKUP.equals(input.getOperationalMode())) {
                    metadatas = ((MatchPlannerBase) matchPlanner).parseCDLMetadata(input);
                } else if (OperationalMode.ENTITY_MATCH.equals(input.getOperationalMode())) {
                    metadatas = ((MatchPlannerBase) matchPlanner).parseEntityMetadata(input);
                } else {
                    String msg = String.format("Entity match operational mode %s is not supported",
                            input.getOperationalMode() == null ? null : input.getOperationalMode().name());
                    throw new UnsupportedOperationException(msg);
                }
            }
            metadataFields = parseMetadataFields();
            outputSchema = columnMetadataService.getAvroSchemaFromColumnMetadatas(metadatas, recordName,
                    dataCloudVersion);
            log.info("Output schema has {} fields for entity match. OperationalMode={}",
                    outputSchema.getFields().size(), input.getOperationalMode());
        } else {
            throw new UnsupportedOperationException("Cannot support cdl bulk match yet.");
        }

        if (inputSchema == null) {
            inputSchema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroPath);
            log.info("Using extracted input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        } else {
            log.info("Using provided input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        }
        Schema newInputSchema = prefixFieldName(inputSchema, outputSchema, "Source_");
        return (Schema) AvroUtils.combineSchemas(newInputSchema, outputSchema)[0];
    }

    private List<String> parseMetadataFields() {
        List<String> fields = new ArrayList<>();
        for (ColumnMetadata column : metadatas) {
            fields.add(column.getAttrName());
        }
        return fields;
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
            // If input attributes have reserved ID fields (AccountId, ContactId
            // and EntityId) conflicting output attributes, drop the ones from
            // input in the output instead of renaming
            if (offendingFields.contains(field.name().toUpperCase()) && InterfaceName.isEntityId(field.name(), false)) {
                continue;
            }
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

}
