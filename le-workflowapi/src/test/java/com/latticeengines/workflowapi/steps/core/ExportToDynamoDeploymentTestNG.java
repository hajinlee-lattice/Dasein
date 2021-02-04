package com.latticeengines.workflowapi.steps.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.datafabric.entitymanager.GenericTableEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.GenericTableEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.dynamo.PrepareTestDynamoConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.TestFrameworkWrapperWorkflowConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.sampletests.SamplePostprocessingStepConfiguration;

public class ExportToDynamoDeploymentTestNG extends
        BaseExportDeploymentTestNG<TestFrameworkWrapperWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportToDynamoDeploymentTestNG.class);

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @Inject
    private DynamoItemService dynamoItemService;

    @Inject
    private FabricDataService dataService;

    private GenericTableEntityMgr entityMgr;

    @Override
    @BeforeClass(groups = "deployment" )
    public void setup() throws Exception {
        super.setup();
        entityMgr = genericTableEntityMgr();
    }

    @Override
    protected TestFrameworkWrapperWorkflowConfiguration generateCreateConfiguration() {
        return generateConfiguration(false);
    }

    @Override
    protected TestFrameworkWrapperWorkflowConfiguration generateUpdateConfiguration() {
        return generateConfiguration(true);
    }

    private TestFrameworkWrapperWorkflowConfiguration generateConfiguration(boolean isUpdateMode) {
        PrepareTestDynamoConfiguration prepareTestDynamoConfig = new PrepareTestDynamoConfiguration(
                "prepareTestDynamo");
        prepareTestDynamoConfig.setUpdateMode(isUpdateMode);
        SamplePostprocessingStepConfiguration postStepConfig = new SamplePostprocessingStepConfiguration(
                "samplePostprocessingStep");

        ExportToDynamoStepConfiguration exportToDynamoStepConfig = new ExportToDynamoStepConfiguration();
        exportToDynamoStepConfig.setCustomerSpace(mainTestCustomerSpace);
        exportToDynamoStepConfig.setDynamoSignature(signature);

        return generateStepTestConfiguration(prepareTestDynamoConfig, "exportToDynamo",
                exportToDynamoStepConfig, postStepConfig);
    }

    @Override
    protected void verifyCreateExport() {
        verifyContactsPerAccount();
        verifyAccountNamePrefix("ACC0001", "ACC1");
        verifyAccountNamePrefix("ACC0007", "ACC1");
    }

    @Override
    protected void verifyUpdateExport() {
        verifyContactsPerAccount();
        verifyAccountNamePrefix("ACC0001", "ACC2");
        verifyAccountNamePrefix("ACC0007", "ACC1");
        verifyBatchGet();
    }

    private void verifyContactsPerAccount() {
        String tableName = String.format("_REPO_GenericTable_RECORD_GenericTableEntity_%s", signature);
        String partitionKey = tenantName + "_" + TABLE_2 + "_ACC0007";
        QuerySpec spec = new QuerySpec().withHashKey("PartitionKey", partitionKey);
        List<Item> items = dynamoItemService.query(tableName, spec);
        Assert.assertTrue(CollectionUtils.isNotEmpty(items));
        Assert.assertEquals(items.size(), NUM_CONTACTS_PER_ACCOUNT);
    }

    private void verifyAccountNamePrefix(String accountId, String prefix) {
        Map<String, Object> result = entityMgr.getByKeyPair(tenantName, TABLE_1, Pair.of(accountId, null));
        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("AccountName"));
        Assert.assertTrue(result.get("AccountName") instanceof String);
        String accountName = result.get("AccountName").toString();
        Assert.assertTrue(accountName.startsWith(prefix));
    }

    private void verifyBatchGet() {
        List<Pair<String, String>> keyPairs = new ArrayList<>();
        keyPairs.add(Pair.of("ACC0001", null));
        keyPairs.add(Pair.of("ACC0002", null));
        keyPairs.add(Pair.of("ACC0003", null));
        keyPairs.add(Pair.of("ACC0004", null));
        keyPairs.add(Pair.of(null, null));
        keyPairs.add(Pair.of("ACC0005", null));
        keyPairs.add(Pair.of("ACC0006", null));
        keyPairs.add(Pair.of("ACC0007", null));
        keyPairs.add(Pair.of("ACC0008", null));
        keyPairs.add(null);

        List<Map<String, Object>> results = entityMgr.getByKeyPairs(tenantName, TABLE_1, keyPairs);
        Assert.assertEquals(results.size(), keyPairs.size());
        for (int i = 0; i < keyPairs.size(); i++) {
            if (i == 4 || i == 9) {
                Assert.assertNull(results.get(i));
            } else {
                Assert.assertNotNull(results.get(i));
            }
        }
    }

    private GenericTableEntityMgr genericTableEntityMgr() {
        return new GenericTableEntityMgrImpl(dataService, signature);
    }

}
