package com.latticeengines.apps.cdl.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.end2end.UpdateTransactionWithAdvancedMatchDeploymentTestNG;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.testframework.CDLWorkflowFrameworkDeploymentTestNGBase;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.PublishDynamoWorkflowConfiguration;
import com.latticeengines.metadata.service.DataUnitService;

/**
 * dpltc deploy -a admin,pls,lp,cdl,eai,metadata,matchapi,workflowapi
 */
public class PublishTableRoleWorkflowDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataUnitService dataUnitService;

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    private List<TableRoleInCollection> tableRoles = Arrays.asList( //
            TableRoleInCollection.ConsolidatedAccount, //
            TableRoleInCollection.ConsolidatedContact //
    );
    private List<String> tableNames = new ArrayList<>();

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        checkpointService.resumeCheckpoint( //
                UpdateTransactionWithAdvancedMatchDeploymentTestNG.CHECK_POINT, //
                CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
        tableRoles.forEach(tableRole -> {
            String tableName = dataCollectionService //
                    .getTableNames(mainCustomerSpace, null, tableRole, null).get(0);
            tableNames.add(tableName);
            Table table = dataCollectionService.getTable(mainCustomerSpace, tableRole, null);
        });
        tableNames.forEach(tableName -> {
            DataUnit dataUnit = dataUnitService.findByNameTypeFromReader(tableName, DataUnit.StorageType.Dynamo);
            Assert.assertNull(dataUnit);
        });
    }

    @Override
    @Test(groups = {"deployment"})
    public void testWorkflow() throws Exception {
        DataCollection.Version version = dataCollectionService.getActiveVersion(mainTestCustomerSpace.toString());
        PublishDynamoWorkflowConfiguration configuration = new PublishDynamoWorkflowConfiguration.Builder() //
                .customer(mainTestCustomerSpace) //
                .tableRoles(tableRoles) //
                .version(version) //
                .dynamoSignature(signature) //
                .build();
        runWorkflow(generateWorkflowTestConfiguration(null, //
                "publishDynamoWorkflow", configuration, null));
        verifyTest();
    }

    @Override
    protected void verifyTest() {
        tableNames.forEach(tableName -> {
            DataUnit dataUnit = dataUnitService.findByNameTypeFromReader(tableName, DataUnit.StorageType.Dynamo);
            Assert.assertNotNull(dataUnit);
        });
    }

}
