package com.latticeengines.workflowapi.flows;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class CustomEventMatchWorkflowDeploymentTestNG extends ImportMatchAndModelWorkflowDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CustomEventMatchWorkflowDeploymentTestNG.class);

    protected static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/customevent";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${common.test.microservice.url}")
    protected String microserviceHostPort;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    private Table accountTable;
    private Table inputTable;
    private Table inputTableWithAccountId;
    private Table inputTableWithoutAccountId;

    private CustomerSpace customerSpace;

    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.CG);
        setupTables();
    }

    @AfterClass(groups = "workflow")
    public void after() throws Exception {
        metadataProxy.deleteTable(customerSpace.toString(), inputTable.getName());
        metadataProxy.deleteTable(customerSpace.toString(), inputTableWithAccountId.getName());
        metadataProxy.deleteTable(customerSpace.toString(), inputTableWithoutAccountId.getName());
        metadataProxy.deleteTable(customerSpace.toString(), accountTable.getName());
    }

    private void setupTables() throws IOException {
        customerSpace = MultiTenantContext.getCustomerSpace();
        accountTable = setupTable(customerSpace, "accountTable.avro", "AccountTable");
        inputTable = setupTable(customerSpace, "inputTable.avro", "InputTable");
        inputTableWithAccountId = setupTable(customerSpace, "inputTableWithAccountId.avro", "InputTableWithAccountId");
        inputTableWithoutAccountId = setupTable(customerSpace, "inputTableWithoutAccountId.avro",
                "InputTableWithoutAccountId");
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        dataCollectionProxy.upsertTable(customerSpace.toString(), accountTable.getName(), //
                TableRoleInCollection.ConsolidatedAccount, version);

    }

    @Test(groups = "workflow", enabled = true)
    public void customEventMatchAllCdl() throws Exception {
        customEventMatch(inputTable, CustomEventModelingType.CDL);
    }

    @Test(groups = "workflow", enabled = false)
    public void customEventMatchWithAccountId() throws Exception {
        customEventMatch(inputTableWithAccountId, CustomEventModelingType.CDL);
    }

    @Test(groups = "workflow", enabled = false)
    public void customEventMatchWithoutAccountId() throws Exception {
        customEventMatch(inputTableWithoutAccountId, CustomEventModelingType.CDL);
    }

    private void customEventMatch(Table table, CustomEventModelingType customEventModelingType) throws Exception {
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        CustomEventMatchWorkflowConfiguration workflowConfig = new CustomEventMatchWorkflowConfiguration.Builder()
                .customer(customerSpace) //
                .microServiceHostPort(microserviceHostPort) //
                .matchInputTableName(table.getName()) //
                .matchAccountIdColumn(InterfaceName.AccountId.name()) //
                .matchRequestSource(MatchRequestSource.MODELING) //
                .matchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .matchColumnSelection(ColumnSelection.Predefined.RTS, "1.0") //
                .dataCloudVersion(getDataCloudVersion()) //
                .matchClientDocument(matchClientDocument) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .setRetainLatticeAccountId(true) //
                .excludePublicDomains(false) //
                .excludeDataCloudAttrs(false) //
                .skipDedupStep(true) //
                .matchGroupId(InterfaceName.Id.name())
                .fetchOnly(true) //
                .sourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.toString()) //
                .build();
        workflowConfig.setCustomEventModelingType(customEventModelingType);
        workflowService.registerJob(workflowConfig, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        waitForCompletion(workflowId);
    }

    private String getDataCloudVersion() {
        return columnMetadataProxy.latestVersion(null).getVersion();
    }

    private Table setupTable(CustomerSpace customerSpace, String fileName, String tableName) throws IOException {
        String hdfsDir = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
        if (!HdfsUtils.fileExists(yarnConfiguration, hdfsDir)) {
            HdfsUtils.mkdir(yarnConfiguration, hdfsDir);
        }
        hdfsDir += "/" + tableName;
        String fullLocalPath = RESOURCE_BASE + "/" + fileName;
        InputStream fileStream = ClassLoader.getSystemResourceAsStream(fullLocalPath);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, hdfsDir + "/" + fileName);

        Table table = MetaDataTableUtils.createTable(yarnConfiguration, tableName, hdfsDir);
        table.getExtracts().get(0).setExtractionTimestamp(System.currentTimeMillis());
        metadataProxy.updateTable(customerSpace.toString(), tableName, table);
        return table;
    }

}
