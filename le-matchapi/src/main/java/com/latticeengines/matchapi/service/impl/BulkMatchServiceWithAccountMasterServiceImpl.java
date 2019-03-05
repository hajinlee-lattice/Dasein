package com.latticeengines.matchapi.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.AccountMasterLookup;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.impl.BulkMatchPlanner;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CascadingBulkMatchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("bulkMatchServiceWithAccountMaster")
public class BulkMatchServiceWithAccountMasterServiceImpl extends BulkMatchServiceWithDerivedColumnCacheImpl {

    private static final String ACCOUNT_MASTER_KEY = "AccountMaster";
    private static final String ACCOUNT_MASTER_LOOKUP_KEY = "AccountMasterLookup";
    private static final String INPUT_AVRO_KEY = "InputAvro";
    private static final String PUBLIC_DOMAIN_KEY = "PublicDomain";

    @Inject
    @Qualifier("accountMasterColumnMetadataService")
    private ColumnMetadataService columnMetadataService;

    @Inject
    @Qualifier("accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Value("${common.microservice.url}")
    protected String microServiceHostPort;

    @Value("${datacloud.match.public_domain.path}")
    protected String publicDomainPath;

    @Value("${datacloud.match.cascading.partitions:8}")
    protected Integer cascadingPartitions;

    @Value("${datacloud.match.cascading.rows.threshold:10000}")
    private Integer cascadingBulkRowsThreshold;

    @Value("${datacloud.match.cascading.container.size:2048}")
    private String cascadingContainerSize;

    @Value("${datacloud.match.cascading.queue.name:Modeling}")
    private String matchQueueName;

    @Inject
    private AccountMaster accountMaster;

    @Inject
    private AccountMasterLookup accountMasterLookup;

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    protected WorkflowProxy workflowProxy;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private BulkMatchPlanner bulkMatchPlanner;

    @Inject
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    protected MatchCommand submitBulkMatchWorkflow(MatchInput input, String hdfsPodId, String rootOperationUid) {
        if (input.isBulkOnly()) {
            MatchCommand matchCommand = submitCascadingBulkMatchWorkflow(input, hdfsPodId, rootOperationUid);
            matchCommand.setCascadingFlow("true");
            return matchCommand;
        }
        return super.submitBulkMatchWorkflow(input, hdfsPodId, rootOperationUid);
    }

    private MatchCommand submitCascadingBulkMatchWorkflow(MatchInput input, String hdfsPodId, String rootOperationUid) {
        dataCloudTenantService.bootstrapServiceTenant();
        String targetPath = hdfsPathBuilder.constructMatchOutputDir(rootOperationUid).toString();
        CascadingBulkMatchWorkflowConfiguration.Builder builder = new CascadingBulkMatchWorkflowConfiguration.Builder();
        String queueName = StringUtils.isEmpty(matchQueueName) ? LedpQueueAssigner.getModelingQueueNameForSubmission()
                : matchQueueName;
        builder = builder //
                .matchInput(input) //
                .hdfsPodId(hdfsPodId) //
                .rootOperationUid(rootOperationUid) //
                .microServiceHostPort(microServiceHostPort) //
                .inputProperties() //
                .targetTableName(((AvroInputBuffer) input.getInputBuffer()).getTableName() + "_match_target") //
                .targetPath(targetPath) //
                .partitions(cascadingPartitions) //
                .jobProperties(getJobProperties(input)) //
                .engine("TEZ") //
                .queue(queueName) //
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

    private Properties getJobProperties(MatchInput input) {
        int mapReduceSize = Integer.parseInt(cascadingContainerSize);
        if (input.getNumRows() > 600_000) {
            mapReduceSize = mapReduceSize * 3;
        } else if (input.getNumRows() > 200_000) {
            mapReduceSize = mapReduceSize * 2;
        }

        Properties jobProperties = new Properties();
        jobProperties.put("mapred.reduce.tasks", "1");
        jobProperties.put("cascading.spill.map.threshold", "100000");
        jobProperties.put("mapreduce.job.running.map.limit", "100");
        jobProperties.put("mapreduce.map.memory.mb", mapReduceSize + "");
        jobProperties.put("mapreduce.reduce.memory.mb", mapReduceSize + "");
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
        extraSources.put(ACCOUNT_MASTER_LOOKUP_KEY + dataVersion.getAccountLookupHdfsVersion(), sourceTable
                .getExtracts().get(0).getPath());
        sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMaster, dataVersion.getAccountMasterHdfsVersion());
        extraSources.put(ACCOUNT_MASTER_KEY + dataVersion.getAccountMasterHdfsVersion(),
                sourceTable.getExtracts().get(0).getPath());
        extraSources.put(PUBLIC_DOMAIN_KEY, publicDomainPath);

        InputBuffer inputBuffer = input.getInputBuffer();
        AvroInputBuffer avroInputBuffer = (AvroInputBuffer) inputBuffer;
        String avroPath = avroInputBuffer.getAvroDir();
        extraSources.put(avroInputBuffer.getTableName() + "_" + INPUT_AVRO_KEY, avroPath);

        return extraSources;
    }

    private CascadingBulkMatchDataflowParameters getDataflowParameters(MatchInput input, String hdfsPodId,
            String outputSchemaPath) {

        DataCloudVersion dataVersion = dataCloudVersionEntityMgr.findVersion(input.getDataCloudVersion());
        CascadingBulkMatchDataflowParameters parameters = new CascadingBulkMatchDataflowParameters();
        parameters.setAccountMasterLookup(ACCOUNT_MASTER_LOOKUP_KEY + dataVersion.getAccountLookupHdfsVersion());
        parameters.setAccountMaster(ACCOUNT_MASTER_KEY + dataVersion.getAccountMasterHdfsVersion());
        parameters.setPublicDomainPath(PUBLIC_DOMAIN_KEY);
        parameters.setInputAvro(((AvroInputBuffer) input.getInputBuffer()).getTableName() + "_" + INPUT_AVRO_KEY);
        parameters.setExcludePublicDomains(input.getExcludePublicDomain());
        parameters.setReturnUnmatched(true);
        parameters.setOutputSchemaPath(outputSchemaPath);
        parameters.setKeyMap(input.getKeyMap());

        ColumnSelection columnSelection = bulkMatchPlanner.parseColumnSelection(input);
        Map<String, Pair<BitCodeBook, List<String>>> decodedParameters = columnSelectionService.getDecodeParameters(
                columnSelection, input.getDataCloudVersion());
        if (decodedParameters != null && decodedParameters.size() > 0)
            parameters.wrapDecodedParameters(decodedParameters);
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

        return outputSchema;
    }

}
