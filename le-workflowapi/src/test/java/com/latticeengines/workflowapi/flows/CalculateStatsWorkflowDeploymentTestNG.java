package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.CalculateStatsWorkflow;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.CalculateStatsWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class CalculateStatsWorkflowDeploymentTestNG extends WorkflowApiFunctionalTestNGBase {

    private final Log log = LogFactory.getLog(CalculateStatsWorkflowDeploymentTestNG.class);

    @Autowired
    private CalculateStatsWorkflow calculateStatsWorkflow;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    protected CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("CalculateStatsTest");

    @BeforeClass(groups = "deployment")
    protected void setupForWorkflow() throws Exception {
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        MultiTenantContext.setTenant(tenant);
        assertNotNull(MultiTenantContext.getTenant());
        setupUsers(DEMO_CUSTOMERSPACE);
        setupCamille(DEMO_CUSTOMERSPACE);
        setupHdfs(DEMO_CUSTOMERSPACE);

        String hdfsPath = "/user/s-analytics/customers/" + DEMO_CUSTOMERSPACE.toString();
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/workflowapi/flows/cdl/master.avro");
        if (is == null) {
            throw new RuntimeException("Failed to load resource cdl.avro.");
        }
        String filePath = hdfsPath + "/tmp.avro";
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, hdfsPath + "/tmp.avro");
        Table table = MetadataConverter.getTable(yarnConfiguration, filePath);
        table.setInterpretation(SchemaInterpretation.Account.name());
        table.setName("Master");
        metadataProxy.createTable(DEMO_CUSTOMERSPACE.toString(), table.getName(), table);

        DataCollection dataCollection = new DataCollection();
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollection.addTable(table);
        dataCollectionProxy.createOrUpdateDataCollection(DEMO_CUSTOMERSPACE.toString(), dataCollection);

        Table retrievedTable = metadataProxy.getTable(DEMO_CUSTOMERSPACE.toString(), table.getName());
        Assert.assertNotNull(retrievedTable);
        System.out.println("attributes are" + Arrays.toString(retrievedTable.getAttributes().toArray()));

        DataCollection retrievedCollection = dataCollectionProxy.getDataCollectionByType(DEMO_CUSTOMERSPACE.toString(),
                DataCollectionType.Segmentation);
        Assert.assertNotNull(retrievedCollection);
    }

    @AfterClass(groups = "deployment")
    protected void cleanUpAfterWorkflow() throws Exception {
        deleteTenantByRestCall(DEMO_CUSTOMERSPACE.toString());
        cleanCamille(DEMO_CUSTOMERSPACE);
        cleanHdfs(DEMO_CUSTOMERSPACE);
    }

    @Test(groups = "deployment", enabled = false)
    public void testWorkflow() throws Exception {
        log.info("customer is " + DEMO_CUSTOMERSPACE.getTenantId());
        CalculateStatsWorkflowConfiguration config = generateConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(calculateStatsWorkflow.name(), config);

        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        Assert.assertEquals(status, BatchStatus.COMPLETED);

        verifyWorkflowResult();
    }

    private CalculateStatsWorkflowConfiguration generateConfiguration() {
        return new CalculateStatsWorkflowConfiguration.Builder() //
                .customer(DEMO_CUSTOMERSPACE) //
                .dataCollectionType(DataCollectionType.Segmentation) //
                .build();
    }

    private void verifyWorkflowResult() {
        verifyDataCollection();
    }

    private void verifyDataCollection() {
        DataCollection dataCollection = dataCollectionProxy.getDataCollectionByType(DEMO_CUSTOMERSPACE.toString(),
                DataCollectionType.Segmentation);
        Assert.assertNotNull(dataCollection);

        Table profileTable = dataCollection.getTable(SchemaInterpretation.Profile);
        Assert.assertNotNull(profileTable);

        StatisticsContainer statisticsContainer = dataCollection.getStatisticsContainer();
        Assert.assertNotNull(statisticsContainer);
    }

}
