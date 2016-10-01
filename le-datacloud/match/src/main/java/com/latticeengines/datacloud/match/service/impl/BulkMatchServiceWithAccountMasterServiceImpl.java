package com.latticeengines.datacloud.match.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.domain.exposed.datacloud.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.propdata.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.impl.AccountMaster;
import com.latticeengines.propdata.core.source.impl.AccountMasterLookup;
import com.latticeengines.propdata.workflow.match.CascadingBulkMatchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("bulkMatchServiceWithAccountMaster")
public class BulkMatchServiceWithAccountMasterServiceImpl extends BulkMatchServiceWithDerivedColumnCacheImpl {

    private static Log log = LogFactory.getLog(BulkMatchServiceWithAccountMasterServiceImpl.class);

    private static final String ACCOUNT_MASTER_KEY = "AccountMaster";
    private static final String ACCOUNT_MASTER_LOOKUP_KEY = "AccountMasterLookup";
    private static final String INPUT_AVRO_KEY = "InputAvro";
    private static final String PUBLIC_DOMAIN_KEY = "PublicDomain";

    @Autowired
    @Qualifier("accountMasterColumnMetadataService")
    private ColumnMetadataService columnMetadataService;

    @Value("${proxy.microservice.rest.endpoint.hostport}")
    protected String microServiceHostPort;

    @Value("${datacloud.match.public_domain.path}")
    protected String publicDomainPath;

    @Value("${datacloud.match.cascading.partitions:8}")
    protected Integer cascadingPartitions;

    @Autowired
    private AccountMaster accountMaster;

    @Autowired
    private AccountMasterLookup accountMasterLookup;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected WorkflowProxy workflowProxy;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private BulkMatchPlanner bulkMatchPlanner;
    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    protected MatchCommand submitBulkMatchWorkflow(MatchInput input, String hdfsPodId, String rootOperationUid) {
        propDataTenantService.bootstrapServiceTenant();
        String targetPath = hdfsPathBuilder.constructMatchOutputDir(rootOperationUid).toString();
        CascadingBulkMatchWorkflowConfiguration.Builder builder = new CascadingBulkMatchWorkflowConfiguration.Builder();
        builder = builder //
                .matchInput(input) //
                .hdfsPodId(hdfsPodId) //
                .rootOperationUid(rootOperationUid) //
                .microServiceHostPort(microServiceHostPort) //
                .inputProperties() //
                .targetTableName(input.getTableName() + "_match_target") //
                .targetPath(targetPath) //
                .partitions(cascadingPartitions) //
                .jobProperties(getJobProperties()) //
                .engine("MR") //
                .setBeanName("cascadingBulkMatchDataflow");

        Schema outputSchema = constructOutputSchema(input, rootOperationUid);
        String outputSchemaPath = writeOutputSchemaAvsc(input, outputSchema, rootOperationUid);
        builder.dataflowParameter(getDataflowParameters(input, hdfsPodId, outputSchemaPath)) //
                .dataflowExtraSources(getDataflowExtraSources(input));

        CascadingBulkMatchWorkflowSubmitter submitter = new CascadingBulkMatchWorkflowSubmitter();
        submitter.setWorkflowProxy(workflowProxy);
        ApplicationId appId = submitter.submit(builder.build());
        return matchCommandService.start(input, appId, rootOperationUid);
    }

    private Properties getJobProperties() {
        Properties jobProperties = new Properties();
        jobProperties.put("mapred.reduce.tasks", "1");
        jobProperties.put("cascading.spill.map.threshold", "100000");
        jobProperties.put("mapreduce.job.running.map.limit", "100");
        return jobProperties;
    }

    private String writeOutputSchemaAvsc(MatchInput input, Schema outputSchema, String rootOperationUid) {
        try {
            String matchAvscPath = hdfsPathBuilder.constructMatchSchemaFileAtRoot(rootOperationUid).toString();
            if (!HdfsUtils.fileExists(yarnConfiguration, matchAvscPath)) {
                HdfsUtils.writeToFile(yarnConfiguration, matchAvscPath, outputSchema.toString());
            }
            return matchAvscPath;
        } catch (Exception e) {
            throw new RuntimeException("Failed to write schema file: " + e.getMessage(), e);
        }
    }

    private Map<String, String> getDataflowExtraSources(MatchInput input) {
        Map<String, String> extraSources = new HashMap<>();
        DataCloudVersion dataVersion = dataCloudVersionEntityMgr.findVersion(input.getDataCloudVersion());

        Table sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMasterLookup,
                dataVersion.getAccountLookupHdfsVersion());
        extraSources.put(ACCOUNT_MASTER_LOOKUP_KEY + dataVersion.getAccountLookupHdfsVersion(),
                sourceTable.getExtracts().get(0).getPath());
        sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMaster, dataVersion.getAccountMasterHdfsVersion());
        extraSources.put(ACCOUNT_MASTER_KEY + dataVersion.getAccountMasterHdfsVersion(),
                sourceTable.getExtracts().get(0).getPath());
        extraSources.put(PUBLIC_DOMAIN_KEY, publicDomainPath);

        InputBuffer inputBuffer = input.getInputBuffer();
        AvroInputBuffer avroInputBuffer = (AvroInputBuffer) inputBuffer;
        String avroPath = avroInputBuffer.getAvroDir();
        extraSources.put(input.getTableName() + "_" + INPUT_AVRO_KEY, avroPath);

        return extraSources;
    }

    private CascadingBulkMatchDataflowParameters getDataflowParameters(MatchInput input, String hdfsPodId,
            String outputSchemaPath) {

        DataCloudVersion dataVersion = dataCloudVersionEntityMgr.findVersion(input.getDataCloudVersion());
        CascadingBulkMatchDataflowParameters parameters = new CascadingBulkMatchDataflowParameters();
        parameters.setAccountMasterLookup(ACCOUNT_MASTER_LOOKUP_KEY + dataVersion.getAccountLookupHdfsVersion());
        parameters.setAccountMaster(ACCOUNT_MASTER_KEY + dataVersion.getAccountMasterHdfsVersion());
        parameters.setPublicDomainPath(PUBLIC_DOMAIN_KEY);
        parameters.setInputAvro(input.getTableName() + "_" + INPUT_AVRO_KEY);
        parameters.setExcludePublicDomains(input.getExcludePublicDomains());
        parameters.setReturnUnmatched(input.getReturnUnmatched());
        parameters.setOutputSchemaPath(outputSchemaPath);
        parameters.setKeyMap(input.getKeyMap());

        return parameters;
    }

    private Schema constructOutputSchema(MatchInput input, String rootOperationUid) {
        Schema outputSchema = null;
        if (input.getPredefinedSelection() != null) {
            outputSchema = columnMetadataService.getAvroSchema(input.getPredefinedSelection(), "PropDataMatchOutput",
                    input.getDataCloudVersion());
        } else {
            ColumnSelection columnSelection = bulkMatchPlanner.parseColumnSelection(input);
            List<ColumnMetadata> columnMetadatas = columnMetadataService.fromSelection(columnSelection,
                    input.getDataCloudVersion());
            outputSchema = columnMetadataService.getAvroSchemaFromColumnMetadatas(columnMetadatas,
                    "PropDataMatchOutput", input.getDataCloudVersion());
        }

        InputBuffer inputBuffer = input.getInputBuffer();
        AvroInputBuffer avroInputBuffer = (AvroInputBuffer) inputBuffer;
        String avroPath = avroInputBuffer.getAvroDir();
        Schema inputSchema = avroInputBuffer.getSchema();
        if (inputSchema == null) {
            inputSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
            log.info("Using extracted input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        } else {
            log.info("Using provited input schema: \n"
                    + JsonUtils.pprint(JsonUtils.deserialize(inputSchema.toString(), JsonNode.class)));
        }
        return outputSchema;
    }
}
