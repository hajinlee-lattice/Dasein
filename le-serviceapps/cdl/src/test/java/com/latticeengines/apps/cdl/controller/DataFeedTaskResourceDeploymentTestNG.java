package com.latticeengines.apps.cdl.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class DataFeedTaskResourceDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskResourceDeploymentTestNG.class);

    private static final String DATA_FEED_TASK_SOURCE = "DataFeedTask_Source";
    private static final String DATA_FEED_TASK_ENTITY = "DataFeedTask_Entity";
    private static final String DATA_FEED_TASK_FEED_TYPE = "DataFeedTask_FeedType";
    private static final String DATA_FEED_TASK_ACTIVE_JOB = "DataFeedTask_ActiveJob";
    private static final String DATA_FEED_TASK_SOURCE_CONFIG = "DataFeedTask_SourceConfig";
    private static final String DATA_FEED_TASK_UNIQUE_ID = "DataFeedTask_UniqueId";
    private static final String TABLE_NAME = NamingUtils.timestamp("Table");
    private static final Date DATE = new Date();

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    private MetadataProxy metadataProxy;

    protected DataFeedTask dataFeedTask;

    @BeforeClass(groups = "deployment")
    public void setup() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironment();

        createTable(TABLE_NAME);

        dataFeedTask = new DataFeedTask();
        dataFeedTask.setSource(DATA_FEED_TASK_SOURCE);
        dataFeedTask.setEntity(DATA_FEED_TASK_ENTITY);
        dataFeedTask.setFeedType(DATA_FEED_TASK_FEED_TYPE);
        dataFeedTask.setActiveJob(DATA_FEED_TASK_ACTIVE_JOB);
        dataFeedTask.setLastImported(DATE);
        dataFeedTask.setLastUpdated(DATE);
        dataFeedTask.setSourceConfig(DATA_FEED_TASK_SOURCE_CONFIG);
        dataFeedTask.setStartTime(DATE);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setUniqueId(DATA_FEED_TASK_UNIQUE_ID + UUID.randomUUID());
        dataFeedTask.setImportTemplate(metadataProxy.getTable(mainCustomerSpace, TABLE_NAME));
    }

    @Test(groups = "deployment")
    public void testCreateDataFeedTask() throws IOException {
        log.info("Creating DataFeedTask for " + mainCustomerSpace);
        dataFeedProxy.createDataFeedTask(mainCustomerSpace, dataFeedTask);

        DataFeedTask dfTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace, DATA_FEED_TASK_SOURCE,
                DATA_FEED_TASK_FEED_TYPE, DATA_FEED_TASK_ENTITY);

        dataFeedTask.setPid(dfTask.getPid());

        assertNotNull(dfTask);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateDataFeedTask")
    public void testCreateOrUpdateDataFeedTask() throws IOException {
        log.info("Create Or Update DataFeedTask for " + mainCustomerSpace);
        dataFeedProxy.createOrUpdateDataFeedTask(mainCustomerSpace, DATA_FEED_TASK_SOURCE, DATA_FEED_TASK_FEED_TYPE,
                DATA_FEED_TASK_ENTITY, TABLE_NAME);

        DataFeedTask dfTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace, DATA_FEED_TASK_SOURCE,
                DATA_FEED_TASK_FEED_TYPE, DATA_FEED_TASK_ENTITY);

        dataFeedTask.setPid(dfTask.getPid());

        assertNotNull(dfTask);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateOrUpdateDataFeedTask")
    public void testUpdateDataFeedTask() throws IOException {
        log.info("Updating DataFeedTask for " + mainCustomerSpace);
        dataFeedTask.setSourceConfig(DATA_FEED_TASK_SOURCE_CONFIG + "Test");
        dataFeedProxy.updateDataFeedTask(mainCustomerSpace, dataFeedTask);

        DataFeedTask dfTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace, dataFeedTask.getUniqueId());

        assertNotNull(dfTask);
        assertEquals(dfTask.getSourceConfig(), DATA_FEED_TASK_SOURCE_CONFIG + "Test");
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateDataFeedTask")
    public void testGetDataFeedTaskWithSameEntity() throws IOException {
        List<DataFeedTask> dfTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(mainCustomerSpace,
                DATA_FEED_TASK_ENTITY);

        assertNotNull(dfTasks);
        assertEquals(dfTasks.size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetDataFeedTaskWithSameEntity")
    public void testRegisterExtract() throws IOException {
        dataFeedProxy.updateDataFeedStatus(mainCustomerSpace, "Active");

        log.info("Register Extract");
        Extract extract = createExtract("Extract_Name");
        List<String> tables = dataFeedProxy.registerExtract(mainCustomerSpace, dataFeedTask.getUniqueId(), TABLE_NAME,
                extract);
        Assert.assertEquals(tables.size(), 1);
        dataFeedProxy.addTableToQueue(mainCustomerSpace, dataFeedTask.getUniqueId(), tables.get(0));
        List<Extract> extractsPendingInQueue = dataFeedProxy.getExtractsPendingInQueue(mainCustomerSpace,
                DATA_FEED_TASK_SOURCE, DATA_FEED_TASK_FEED_TYPE, DATA_FEED_TASK_ENTITY);

        assertNotNull(extractsPendingInQueue);
        assertTrue(extractsPendingInQueue.size() == 1);
    }

    @Test(groups = "deployment", dependsOnMethods = "testRegisterExtract")
    public void testRegisterExtracts() throws IOException {
        log.info("Register Extracts");
        List<Extract> extracts = new ArrayList<>();
        extracts.add(createExtract("Extract_Name_1"));
        extracts.add(createExtract("Extract_Name_2"));
        List<String> tables = dataFeedProxy.registerExtracts(mainCustomerSpace, dataFeedTask.getUniqueId(), TABLE_NAME,
                extracts);
        Assert.assertEquals(tables.size(), 2);
        dataFeedProxy.addTablesToQueue(mainCustomerSpace, dataFeedTask.getUniqueId(), tables);
        List<Extract> extractsPendingInQueue = dataFeedProxy.getExtractsPendingInQueue(mainCustomerSpace,
                DATA_FEED_TASK_SOURCE, DATA_FEED_TASK_FEED_TYPE, DATA_FEED_TASK_ENTITY);

        assertNotNull(extractsPendingInQueue);
        assertTrue(extractsPendingInQueue.size() == 3);
    }

    private Extract createExtract(String name) {
        Extract extract = new Extract();
        extract.setName(name);
        extract.setTenant(mainTestTenant);
        extract.setExtractionTimestamp(System.currentTimeMillis());
        extract.setPath("/" + name);

        return extract;
    }
}
