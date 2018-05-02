package com.latticeengines.workflowapi.steps.core;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.workflowapi.flows.testflows.dynamo.TestDynamoWorkflowConfiguration;

public class ExportToDynamoDeploymentTestNG extends BaseExportDeploymentTestNG<TestDynamoWorkflowConfiguration> {

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @Inject
    private DynamoItemService dynamoItemService;

    @Override
    protected TestDynamoWorkflowConfiguration generateCreateConfiguration() {
        TestDynamoWorkflowConfiguration.Builder builder = new TestDynamoWorkflowConfiguration.Builder();
        return builder //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(mainTestCustomerSpace) //
                .dynamoSignature(signature) //
                .updateMode(false) //
                .build();
    }

    @Override
    protected TestDynamoWorkflowConfiguration generateUpdateConfiguration() {
        TestDynamoWorkflowConfiguration.Builder builder = new TestDynamoWorkflowConfiguration.Builder();
        return builder //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(mainTestCustomerSpace) //
                .dynamoSignature(signature) //
                .updateMode(true) //
                .build();
    }

    @Override
    protected void verifyCreateExport() {
        String tableName = String.format("_REPO_GenericTable_RECORD_GenericTableEntity_%s", signature);
        String tenantName = mainTestCustomerSpace.getTenantId();
        QuerySpec spec = new QuerySpec().withHashKey("PartitionKey", tenantName + "_Table2_ACC0007");
        List<Item> items = dynamoItemService.query(tableName, spec);
        Assert.assertTrue(CollectionUtils.isNotEmpty(items));
        Assert.assertEquals(items.size(), NUM_CONTACTS_PER_ACCOUNT);
    }

    @Override
    protected void verifyUpdateExport() {
        String tableName = String.format("_REPO_GenericTable_RECORD_GenericTableEntity_%s", signature);
        String tenantName = mainTestCustomerSpace.getTenantId();
        QuerySpec spec = new QuerySpec().withHashKey("PartitionKey", tenantName + "_Table2_ACC0007");
        List<Item> items = dynamoItemService.query(tableName, spec);
        Assert.assertTrue(CollectionUtils.isNotEmpty(items));
        Assert.assertEquals(items.size(), NUM_CONTACTS_PER_ACCOUNT);
    }
}
