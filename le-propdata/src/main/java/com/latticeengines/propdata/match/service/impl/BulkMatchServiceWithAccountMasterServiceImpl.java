package com.latticeengines.propdata.match.service.impl;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.propdata.match.InputBuffer;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.impl.AccountMaster;
import com.latticeengines.propdata.core.source.impl.AccountMasterIndex;
import com.latticeengines.propdata.core.source.impl.PublicDomain;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.util.MatchUtils;
import com.latticeengines.propdata.workflow.match.CascadingBulkMatchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("bulkMatchServiceWithAccountMaster")
public class BulkMatchServiceWithAccountMasterServiceImpl extends BulkMatchServiceWithDerivedColumnCacheImpl {

    private static Log log = LogFactory.getLog(BulkMatchServiceWithAccountMasterServiceImpl.class);

    private static final String DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING = "2.";

    private static final String ACCOUNT_MASTER_KEY = "AccountMaster";
    private static final String ACCOUNT_MASTER_INDEX_KEY = "AccountMasterIndex";
    private static final String INPUT_AVRO_KEY = "InputAvro";
    private static final String PUBLIC_DOMAIN_KEY = "PublicDomain";

    @Resource(name = "columnMetadataServiceDispatch")
    private ColumnMetadataService columnMetadataService;

    @Value("${proxy.microservice.rest.endpoint.hostport}")
    protected String microServiceHostPort;

    @Autowired
    private AccountMaster accountMaster;

    @Autowired
    private PublicDomain publicDomain;

    @Autowired
    private AccountMasterIndex accountMasterIndex;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected WorkflowProxy workflowProxy;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Override
    public boolean accept(String version) {
        if (!StringUtils.isEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING)) {
            return true;
        }

        return false;
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

    private String writeOutputSchemaAvsc(MatchInput input, Schema outputSchema, String rootOperationUid) {
        try {
            String matchAvscPath = hdfsPathBuilder.constructMatchSchemaFile(rootOperationUid).toString();
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
        String dataVersion = MatchUtils.getDataVersion(input.getDataCloudVersion());
        Table sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMasterIndex, dataVersion);
        extraSources.put(ACCOUNT_MASTER_INDEX_KEY, sourceTable.getExtracts().get(0).getPath());
        sourceTable = hdfsSourceEntityMgr.getTableAtVersion(accountMaster, dataVersion);
        extraSources.put(ACCOUNT_MASTER_KEY, sourceTable.getExtracts().get(0).getPath());
        sourceTable = hdfsSourceEntityMgr.getTableAtVersion(publicDomain, "0");
        extraSources.put(PUBLIC_DOMAIN_KEY, sourceTable.getExtracts().get(0).getPath());

        InputBuffer inputBuffer = input.getInputBuffer();
        AvroInputBuffer avroInputBuffer = (AvroInputBuffer) inputBuffer;
        String avroPath = avroInputBuffer.getAvroDir();
        extraSources.put(INPUT_AVRO_KEY, avroPath);

        return extraSources;
    }

    private CascadingBulkMatchDataflowParameters getDataflowParameters(MatchInput input, String hdfsPodId,
            String outputSchemaPath) {
        CascadingBulkMatchDataflowParameters parameters = new CascadingBulkMatchDataflowParameters();
        parameters.setAccountMasterIndex(ACCOUNT_MASTER_INDEX_KEY);
        parameters.setAccountMaster(ACCOUNT_MASTER_KEY);
        parameters.setInputAvro(INPUT_AVRO_KEY);
        parameters.setPublicDomainPath(PUBLIC_DOMAIN_KEY);
        parameters.setExcludePublicDomains(input.getExcludePublicDomains());
        parameters.setReturnUnmatched(input.getReturnUnmatched());
        parameters.setOutputSchemaPath(outputSchemaPath);
        parameters.setKeyMap(input.getKeyMap());

        return parameters;
    }

    private Schema constructOutputSchema(MatchInput input, String rootOperationUid) {
        Schema outputSchema = columnMetadataService.getAvroSchema(input.getPredefinedSelection(),
                "PropDataMatchOutput", input.getDataCloudVersion());

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
        // inputSchema = prefixFieldName(inputSchema, "Source_");
        // Schema combinedSchema = (Schema)
        // AvroUtils.combineSchemas(inputSchema, outputSchema)[0];

        // return combinedSchema;
        return outputSchema;
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

}
