package com.latticeengines.datacloud.yarn.runtime;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_REMOVED;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import com.latticeengines.datacloud.match.service.impl.BeanDispatcherImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

@Component("processorContext")
public class ProcessorContext {

    private static final Log log = LogFactory.getLog(ProcessorContext.class);

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

    @Value("${datacloud.match.num.threads}")
    private Integer sqlThreadPool;

    @Value("${datacloud.match.bulk.group.size}")
    private Integer sqlGroupSize;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    @Value("${datacloud.yarn.actors.num.threads}")
    private int actorsThreadPool;

    @Value("${datacloud.yarn.actors.group.size}")
    private int actorsGroupSize;

    private static final Long TIME_OUT_PER_10K = TimeUnit.MINUTES.toMillis(20);

    private DataCloudJobConfiguration jobConfiguration;

    private Tenant tenant;
    private Predefined predefinedSelection;
    private ColumnSelection customizedSelection;
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
    private Boolean excludeUnmatchedWithPublicDomain;
    private Boolean publicDomainAsNormalDomain;
    private boolean useProxy = false;
    private String decisionGraph;
    private int splits = 1;

    private BlockDivider divider;
    private String blockRootDir;
    private Integer numThreads;

    private AtomicInteger rowsProcessed = new AtomicInteger(0);

    private long timeOut;

    private DataCloudProcessor dataCloudProcessor;

    private boolean useRemoteDnB;

    private boolean matchDebugEnabled;

    private long recordTimeOut;

    private boolean disableDunsValidation;

    private MatchInput originalInput;

    public DataCloudJobConfiguration getJobConfiguration() {
        return jobConfiguration;
    }

    public DataCloudProcessor getDataCloudProcessor() {
        return dataCloudProcessor;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    public ColumnSelection getCustomizedSelection() {
        return customizedSelection;
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

    public String getOutputAvroGlob() {
        return hdfsPathBuilder.constructMatchBlockAvroGlob(rootOperationUid, blockOperationUid);
    }

    public String getOutputAvro(int split) {
        return hdfsPathBuilder.constructMatchBlockSplitAvro(rootOperationUid, blockOperationUid, split).toString();
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

    public Boolean getExcludeUnmatchedWithPublicDomain() {
        return excludeUnmatchedWithPublicDomain;
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

    public void initialize(DataCloudProcessor dataCloudProcessor, DataCloudJobConfiguration jobConfiguration)
            throws Exception {
        this.jobConfiguration = jobConfiguration;
        this.dataCloudProcessor = dataCloudProcessor;
        dataCloudVersion = jobConfiguration.getMatchInput().getDataCloudVersion();

        receivedAt = new Date();

        podId = jobConfiguration.getHdfsPodId();
        originalInput = jobConfiguration.getMatchInput();

        returnUnmatched = true;
        excludeUnmatchedWithPublicDomain = jobConfiguration.getMatchInput().getExcludeUnmatchedWithPublicDomain();
        publicDomainAsNormalDomain = jobConfiguration.getMatchInput().isPublicDomainAsNormalDomain();
        HdfsPodContext.changeHdfsPodId(podId);
        log.info("Use PodId=" + podId);

        rootOperationUid = jobConfiguration.getRootOperationUid();
        blockOperationUid = jobConfiguration.getBlockOperationUid();
        outputJson = hdfsPathBuilder.constructMatchBlockOutputFile(rootOperationUid, blockOperationUid).toString();

        blockRootDir = hdfsPathBuilder.constructMatchBlockDir(rootOperationUid, blockOperationUid).toString();
        if (jobConfiguration.getMatchInput().getSplitsPerBlock() != null) {
            splits = Math.max(1, jobConfiguration.getMatchInput().getSplitsPerBlock());
            log.info("Generating " + splits + " splits.");
        }

        CustomerSpace space = jobConfiguration.getCustomerSpace();
        tenant = new Tenant(space.toString());
        predefinedSelection = jobConfiguration.getMatchInput().getPredefinedSelection();
        customizedSelection = jobConfiguration.getMatchInput().getCustomSelection();

        decisionGraph = jobConfiguration.getMatchInput().getDecisionGraph();
        if (Boolean.TRUE.equals(jobConfiguration.getMatchInput().getFetchOnly())) {
            useRemoteDnB = false;
        } else {
            if (jobConfiguration.getMatchInput().getUseRemoteDnB() != null) {
                useRemoteDnB = jobConfiguration.getMatchInput().getUseRemoteDnB();
            } else {
                useRemoteDnB = zkConfigurationService.fuzzyMatchEnabled(space);
            }
            useRemoteDnB = useRemoteDnB && MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion);
            useRemoteDnB = useRemoteDnB && zkConfigurationService.useRemoteDnBGlobal();
        }
        log.info("Use remote DnB ? " + useRemoteDnB);
        if (StringUtils.isEmpty(decisionGraph)) {
            decisionGraph = defaultGraph;
            log.info("Overwrite decision graph be default value " + decisionGraph);
        }
        log.info("Use decision graph " + decisionGraph);

        keyMap = jobConfiguration.getMatchInput().getKeyMap();
        blockSize = jobConfiguration.getBlockSize();
        timeOut = Math.max(Math.round(TIME_OUT_PER_10K * blockSize / 10000.0), TimeUnit.MINUTES.toMillis(60));
        this.recordTimeOut = timeOut;
        if (useRemoteDnB) {
            timeOut = timeOut * 2;
        }
        log.info(String.format("Set timeout to be %.2f minutes for %d records", (timeOut / 60000.0), blockSize));

        useProxy = Boolean.TRUE.equals(jobConfiguration.getMatchInput().getUseRealTimeProxy());
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
                groupSize = 10_000;
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
        outputSchema = constructOutputSchema("PropDataMatchOutput_" + blockOperationUid.replace("-", "_"),
                jobConfiguration.getMatchInput().getDataCloudVersion());

        // sequence is very important in outputschema
        // am output attr -> dedupe -> debug
        // the same sequence will be used in writeDataToAvro
        log.info("Need to prepare for dedupe: " + originalInput.isPrepareForDedupe());
        if (MatchRequestSource.MODELING.equals(originalInput.getRequestSource()) && originalInput.isPrepareForDedupe()) {
            outputSchema = appendDedupeHelpers(outputSchema);
        }

        matchDebugEnabled = zkConfigurationService.isMatchDebugEnabled(space)
                || jobConfiguration.getMatchInput().isMatchDebugEnabled();
        jobConfiguration.getMatchInput().setMatchDebugEnabled(matchDebugEnabled);
        log.info("Match Debug Enabled=" + matchDebugEnabled);
        if (matchDebugEnabled) {
            outputSchema = appendDebugSchema(outputSchema);
        }

        disableDunsValidation = jobConfiguration.getMatchInput() != null
                && jobConfiguration.getMatchInput().isDisableDunsValidation();
        log.info(String.format("Duns validation is disabled: %b", disableDunsValidation));
        cleanup();
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
    private Schema constructOutputSchema(String recordName, String dataCloudVersion) {
        Schema outputSchema;
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
        if (predefinedSelection == null) {
            List<ColumnMetadata> metadatas = columnMetadataService.fromSelection(customizedSelection, dataCloudVersion);
            outputSchema = columnMetadataService.getAvroSchemaFromColumnMetadatas(metadatas, recordName,
                    dataCloudVersion);
        } else {
            outputSchema = columnMetadataService.getAvroSchema(predefinedSelection, recordName, dataCloudVersion);
        }
        if (inputSchema == null) {
            inputSchema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroPath);
            log.info("Using extracted input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        } else {
            log.info("Using provided input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        }
        inputSchema = prefixFieldName(inputSchema, outputSchema, "Source_");
        return (Schema) AvroUtils.combineSchemas(inputSchema, outputSchema)[0];
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

}
