package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

public class DataFeedTaskResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskResourceDeploymentTestNG.class);

    private static final String DATA_FEED_TASK_SOURCE = "DataFeedTask_Source";
    private static final String DATA_FEED_TASK_ENTITY = "DataFeedTask_Entity";
    private static final String DATA_FEED_TASK_FEED_TYPE = "DataFeedTask_FeedType";
    private static final String DATA_FEED_TASK_ACTIVE_JOB = "DataFeedTask_ActiveJob";
    private static final String DATA_FEED_TASK_SOURCE_CONFIG = "DataFeedTask_SourceConfig";
    private static final String DATA_FEED_TASK_UNIQUE_ID = "DataFeedTask_UniqueId";
    private static final Date DATE = new Date();

    @Autowired
    protected DataFeedProxy dataFeedProxy;

    protected DataFeedTask dataFeedTask;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();

        dataFeedTask = new DataFeedTask();
        dataFeedTask.setSource(DATA_FEED_TASK_SOURCE);
        dataFeedTask.setEntity(DATA_FEED_TASK_ENTITY);
        dataFeedTask.setFeedType(DATA_FEED_TASK_FEED_TYPE);
        dataFeedTask.setActiveJob(DATA_FEED_TASK_ACTIVE_JOB);
        dataFeedTask.setLastImported(DATE);
        dataFeedTask.setSourceConfig(DATA_FEED_TASK_SOURCE_CONFIG);
        dataFeedTask.setStartTime(DATE);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setUniqueId(DATA_FEED_TASK_UNIQUE_ID + UUID.randomUUID());
        dataFeedTask.setImportTemplate(metadataProxy.getTable(customerSpace1, TABLE1));
    }

    @Test(groups = "deployment")
    public void testCreateDataFeedTask() throws IOException {
        log.info("Creating DataFeedTask for " + customerSpace1);
        dataFeedProxy.createDataFeedTask(customerSpace1, dataFeedTask);

        DataFeedTask dfTask = dataFeedProxy.getDataFeedTask(customerSpace1, DATA_FEED_TASK_SOURCE,
                DATA_FEED_TASK_FEED_TYPE, DATA_FEED_TASK_ENTITY);

        dataFeedTask.setPid(dfTask.getPid());

        assertNotNull(dfTask);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateDataFeedTask")
    public void testCreateOrUpdateDataFeedTask() throws IOException {
        log.info("Create Or Update DataFeedTask for " + customerSpace1);
        dataFeedProxy.createOrUpdateDataFeedTask(customerSpace1, DATA_FEED_TASK_SOURCE, DATA_FEED_TASK_FEED_TYPE,
                DATA_FEED_TASK_ENTITY, TABLE1);

        DataFeedTask dfTask = dataFeedProxy.getDataFeedTask(customerSpace1, DATA_FEED_TASK_SOURCE,
                DATA_FEED_TASK_FEED_TYPE, DATA_FEED_TASK_ENTITY);

        dataFeedTask.setPid(dfTask.getPid());

        assertNotNull(dfTask);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateOrUpdateDataFeedTask")
    public void testUpdateDataFeedTask() throws IOException {
        log.info("Updating DataFeedTask for " + customerSpace1);
        dataFeedTask.setSourceConfig(DATA_FEED_TASK_SOURCE_CONFIG + "Test");
        dataFeedProxy.updateDataFeedTask(customerSpace1, dataFeedTask);

        DataFeedTask dfTask = dataFeedProxy.getDataFeedTask(customerSpace1, dataFeedTask.getUniqueId());

        assertNotNull(dfTask);
        assertEquals(dfTask.getSourceConfig(), DATA_FEED_TASK_SOURCE_CONFIG + "Test");
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateDataFeedTask")
    public void testGetDataFeedTaskWithSameEntity() throws IOException {
        List<DataFeedTask> dfTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace1, DATA_FEED_TASK_ENTITY);

        assertNotNull(dfTasks);
        assertEquals(dfTasks.size(),1);
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetDataFeedTaskWithSameEntity")
    public void testRegisterExtract() throws IOException {
        dataFeedProxy.updateDataFeedStatus(customerSpace1, "Active");

        log.info("Register Extract");
        Extract extract = createExtract("Extract_Name");
        dataFeedProxy.registerExtract(customerSpace1, dataFeedTask.getUniqueId(), TABLE1, extract);

        List<Extract> extractsPendingInQueue = dataFeedProxy.getExtractsPendingInQueue(customerSpace1, DATA_FEED_TASK_SOURCE,
                DATA_FEED_TASK_FEED_TYPE, DATA_FEED_TASK_ENTITY);

        assertNotNull(extractsPendingInQueue);
        assertTrue(extractsPendingInQueue.size() == 1);
    }

    @Test(groups = "deployment", dependsOnMethods = "testRegisterExtract")
    public void testRegisterExtracts() throws IOException {
        log.info("Register Extracts");
        List<Extract> extracts = new ArrayList<>();
        extracts.add(createExtract("Extract_Name_1"));
        extracts.add(createExtract("Extract_Name_2"));
        dataFeedProxy.registerExtracts(customerSpace1, dataFeedTask.getUniqueId(), TABLE1, extracts);

        List<Extract> extractsPendingInQueue = dataFeedProxy.getExtractsPendingInQueue(customerSpace1,
                DATA_FEED_TASK_SOURCE, DATA_FEED_TASK_FEED_TYPE, DATA_FEED_TASK_ENTITY);

        assertNotNull(extractsPendingInQueue);
        assertTrue(extractsPendingInQueue.size() == 3);
    }

    private Extract createExtract(String name) {
        Extract extract = new Extract();
        extract.setName(name);
        extract.setTenant(tenant1);
        extract.setExtractionTimestamp(System.currentTimeMillis());
        extract.setPath("/" + name);

        return extract;
    }
}