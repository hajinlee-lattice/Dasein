package com.latticeengines.datacloud.yarn.runtime;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
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
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
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

    @Value("${datacloud.match.fuzzymatch.decision.graph}")
    private String fuzzyMatchGraph;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    @Value("${datacloud.yarn.actors.num.threads}")
    private int fuzzyThreadPool;

    @Value("${datacloud.yarn.actors.group.size}")
    private int fuzzyGroupSize;

    private static final Long TIME_OUT_PER_10K = TimeUnit.MINUTES.toMillis(20);

    private DataCloudJobConfiguration jobConfiguration;

    private Tenant tenant;
    private Predefined predefinedSelection;
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
    private String decisionGraph;

    private BlockDivider divider;
    private String blockRootDir;
    private Integer groupSize;
    private Integer numThreads;

    private AtomicInteger rowsProcessed = new AtomicInteger(0);

    private long timeOut;

    private DataCloudProcessor dataCloudProcessor;

    private boolean fuzzyMatchEnabled;

    public Integer getSqlThreadPool() {
        return sqlThreadPool;
    }

    public Integer getSqlGroupSize() {
        return sqlGroupSize;
    }

    public String getFuzzyMatchGraph() {
        return fuzzyMatchGraph;
    }

    public String getDefaultGraph() {
        return defaultGraph;
    }

    public int getFuzzyThreadPool() {
        return fuzzyThreadPool;
    }

    public int getFuzzyGroupSize() {
        return fuzzyGroupSize;
    }

    public static Long getTimeOutPer10k() {
        return TIME_OUT_PER_10K;
    }

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

    public String getAvroPath() {
        return avroPath;
    }

    public String getOutputAvro() {
        return outputAvro;
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

    public Schema getInputSchema() {
        return inputSchema;
    }

    public MatchInput getMatchInput() {
        return matchInput;
    }

    public void setMatchInput(MatchInput matchInput) {
        this.matchInput = matchInput;
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

    public ConcurrentSkipListSet<String> getFieldsWithNoMetadata() {
        return fieldsWithNoMetadata;
    }

    public Map<String, AccountMasterColumn> getAccountMasterColumnMap() {
        return accountMasterColumnMap;
    }

    public void setAccountMasterColumnMap(Map<String, AccountMasterColumn> accountMasterColumnMap) {
        this.accountMasterColumnMap = accountMasterColumnMap;
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

    public String getBlockRootDir() {
        return blockRootDir;
    }

    public Integer getGroupSize() {
        return groupSize;
    }

    public Integer getNumThreads() {
        return numThreads;
    }

    public long getTimeOut() {
        return timeOut;
    }

    public boolean isFuzzyMatchEnabled() {
        return fuzzyMatchEnabled;
    }

    public void setFuzzyMatchEnabled(boolean fuzzyMatchEnabled) {
        this.fuzzyMatchEnabled = fuzzyMatchEnabled;
    }

    public void initialize(DataCloudProcessor dataCloudProcessor, DataCloudJobConfiguration jobConfiguration)
            throws Exception {
        this.jobConfiguration = jobConfiguration;
        this.dataCloudProcessor = dataCloudProcessor;
        dataCloudVersion = jobConfiguration.getDataCloudVersion();

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

        blockRootDir = hdfsPathBuilder.constructMatchBlockDir(rootOperationUid, blockOperationUid).toString();

        CustomerSpace space = jobConfiguration.getCustomerSpace();
        tenant = new Tenant(space.toString());
        predefinedSelection = jobConfiguration.getPredefinedSelection();
        customizedSelection = jobConfiguration.getCustomizedSelection();

        decisionGraph = jobConfiguration.getDecisionGraph();
        fuzzyMatchEnabled = zkConfigurationService.fuzzyMatchEnabled(space) || jobConfiguration.isFuzzyMatchEnabled();
        log.info("Fuzzy match enabled ? " + fuzzyMatchEnabled);
        if (StringUtils.isEmpty(decisionGraph)) {
            if (fuzzyMatchEnabled) {
                decisionGraph = fuzzyMatchGraph;
            } else {
                decisionGraph = defaultGraph;
            }
            log.info("Overwrite decision graph be default value " + decisionGraph);
        }
        log.info("Use decision graph " + decisionGraph);

        keyMap = jobConfiguration.getKeyMap();
        blockSize = jobConfiguration.getBlockSize();
        timeOut = Math.max(Math.round(TIME_OUT_PER_10K * blockSize / 10000.0), TimeUnit.MINUTES.toMillis(30));
        log.info(String.format("Set timeout to be %.2f minutes for %d records", (timeOut / 60000.0), blockSize));

        useProxy = Boolean.TRUE.equals(jobConfiguration.getUseRealTimeProxy());
        if (useProxy) {
            String overwritingProxyUrl = jobConfiguration.getRealTimeProxyUrl();
            if (StringUtils.isNotEmpty(overwritingProxyUrl)) {
                matchProxy.setHostport(overwritingProxyUrl);
            }
            log.info("Using real-time match proxy at " + matchProxy.getHostport());
        }

        groupSize = jobConfiguration.getGroupSize();
        numThreads = jobConfiguration.getThreadPoolSize();
        if (MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion)) {
            groupSize = fuzzyGroupSize;
            numThreads = fuzzyThreadPool;
            if (fuzzyMatchEnabled) {
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
                jobConfiguration.getDataCloudVersion());

        cleanup();
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
