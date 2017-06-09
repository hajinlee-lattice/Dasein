package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;

import java.util.Collections;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class DataIngestionEnd2EndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(DataIngestionEnd2EndDeploymentTestNG.class);

    private static final String DATA_COLLECTION_NAME = "DATA_COLLECTION_NAME";

    private static final String DATA_FEED_NAME = "DATA_FEED_NAME";

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private DataFeedProxy datafeedProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    private Tenant firstTenant;

    @BeforeClass(groups = { "deployment.cdl" })
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        firstTenant = testBed.getMainTestTenant();

        log.info("Test environment setup finished.");
        createDataFeed();
    }

    @Test(groups = { "deployment.cdl" }, enabled = true)
    public void importData() {

    }

    @Test(groups = { "deployment.cdl" }, enabled = true, dependsOnMethods = "importData")
    public void consolidateAndPublish() {
        log.info("Start consolidating data ...");
        ResponseDocument<?> response = restTemplate.postForObject(String.format("%s/pls/%s/datafeeds/%s/consolidate",
                getRestAPIHostPort(), DataCollectionType.Segmentation, DATA_FEED_NAME), null, ResponseDocument.class);
        String appId = new ObjectMapper().convertValue(response.getResult(), String.class);
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private void createDataFeed() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setName(DATA_COLLECTION_NAME);
        Table table = new Table();
        table.setName(SchemaInterpretation.Account.name());
        table.setInterpretation(SchemaInterpretation.Account.name());
        dataCollection.setTables(Collections.singletonList(table));
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollectionProxy.createOrUpdateDataCollection(firstTenant.getId(), dataCollection);

        DataFeed datafeed = new DataFeed();
        datafeed.setName(DATA_FEED_NAME);
        datafeed.setStatus(Status.Active);
        datafeed.setActiveExecutionId(1L);
        datafeed.setDataCollection(dataCollection);
        dataCollection.addDataFeed(datafeed);

        DataFeedExecution execution = new DataFeedExecution();
        execution.setDataFeed(datafeed);
        execution.setStatus(DataFeedExecution.Status.Active);
        datafeed.addExeuction(execution);

        Table importTable = new Table();
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(firstTenant);

        Table dataTable = new Table();
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(firstTenant);

        DataFeedTask task = new DataFeedTask();
        task.setDataFeed(datafeed);
        task.setActiveJob("1");
        task.setEntity(SchemaInterpretation.Account.name());
        task.setSource("VDB");
        task.setStatus(DataFeedTask.Status.Active);
        task.setSourceConfig("config");
        task.setImportTemplate(importTable);
        task.setImportData(dataTable);
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        datafeed.addTask(task);
        datafeedProxy.createDataFeed(firstTenant.getId(), datafeed);
    }

    @Test(groups = { "deployment.cdl" }, enabled = true, dependsOnMethods = "consolidateAndPublish")
    public void finalize() {

    }

    @Test(groups = { "deployment.cdl" }, enabled = true, dependsOnMethods = "finalize")
    public void querySegment() {

    }
}
