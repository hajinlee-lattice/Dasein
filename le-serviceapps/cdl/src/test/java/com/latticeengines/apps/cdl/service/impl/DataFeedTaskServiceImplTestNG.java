package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class DataFeedTaskServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataFeedTaskService datafeedTaskService;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    private DataFeedTask dataFeedTask = new DataFeedTask();

    private DataFeed datafeed = new DataFeed();

    private CustomerSpace customerSpace;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCreate() {
        customerSpace = MultiTenantContext.getCustomerSpace();
        DataCollection dataCollection = new DataCollection();
        dataCollection.setName(NamingUtils.timestamp("DATA_COLLECTION_NAME"));
        dataCollection.setTenant(MultiTenantContext.getTenant());
        dataCollection.setVersion(DataCollection.Version.Blue);
        dataCollectionEntityMgr.create(dataCollection);

        Table importTable = new Table(TableType.IMPORTTABLE);
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(MultiTenantContext.getTenant());
        tableEntityMgr.create(importTable);

        Table dataTable = new Table(TableType.DATATABLE);
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(MultiTenantContext.getTenant());
        Extract e = new Extract();
        e.setName("extract");
        e.setPath("abc");
        e.setExtractionTimestamp(1L);
        e.setProcessedRecords(100L);
        dataTable.addExtract(e);
        tableEntityMgr.create(dataTable);

        datafeed.setName(NamingUtils.timestamp("datafeed"));
        datafeed.setStatus(DataFeed.Status.ProcessAnalyzing);
        datafeed.setDataCollection(dataCollection);

        dataFeedTask.setDataFeed(datafeed);
        dataFeedTask.setActiveJob("3");
        dataFeedTask.setEntity(SchemaInterpretation.Account.name());
        dataFeedTask.setSource("SFDC");
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setSourceConfig("config");
        dataFeedTask.setImportTemplate(importTable);
        dataFeedTask.setImportData(dataTable);
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date());
        dataFeedTask.setLastUpdated(new Date());
        dataFeedTask.setTemplateDisplayName("DisplayA");
        dataFeedTask.setUniqueId(UUID.randomUUID().toString());
        datafeed.addTask(dataFeedTask);
        dataFeedService.createDataFeed(MultiTenantContext.getTenant().getId(), dataCollection.getName(), datafeed);
        dataFeedTask = datafeedTaskService.getDataFeedTask(customerSpace.toString(), dataFeedTask.getUniqueId());
        Assert.assertNotNull(dataFeedTask);
    }

    private void createS3ImportSystem() {
        S3ImportSystem system1= new S3ImportSystem();
        system1.setTenant(mainTestTenant);
        system1.setName("SYSTEM1");
        system1.setDisplayName("SYSTEM1");
        system1.setSystemType(S3ImportSystem.SystemType.Salesforce);
        s3ImportSystemService.createS3ImportSystem(mainCustomerSpace, system1);

        S3ImportSystem system2= new S3ImportSystem();
        system2.setTenant(mainTestTenant);
        system2.setName("SYSTEM2");
        system2.setDisplayName("SYSTEM2");
        system2.setSystemType(S3ImportSystem.SystemType.Other);
        s3ImportSystemService.createS3ImportSystem(mainCustomerSpace, system2);

        S3ImportSystem system3= new S3ImportSystem();
        system3.setTenant(mainTestTenant);
        system3.setName("SYSTEM3");
        system3.setDisplayName("SYSTEM3");
        system3.setSystemType(S3ImportSystem.SystemType.Other);
        s3ImportSystemService.createS3ImportSystem(mainCustomerSpace, system3);

        system3.setPriority(1);
        s3ImportSystemService.updateS3ImportSystem(mainCustomerSpace, system3);
    }

    private void createMoreDataFeedTask(String systemName) {
        DataFeedTask dataFeedTask = new DataFeedTask();
        Table templateTable = new Table(TableType.IMPORTTABLE);
        templateTable.setName(systemName + "_importTable");
        templateTable.setDisplayName(templateTable.getName());
        templateTable.setTenant(MultiTenantContext.getTenant());
        tableEntityMgr.create(templateTable);

        dataFeedTask.setDataFeed(datafeed);
        dataFeedTask.setActiveJob("3");
        dataFeedTask.setEntity(SchemaInterpretation.Account.name());
        dataFeedTask.setSource("File");
        dataFeedTask.setFeedType(systemName + "_AccountData");
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setSourceConfig("config");
        dataFeedTask.setImportTemplate(templateTable);
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date());
        dataFeedTask.setLastUpdated(new Date());
        dataFeedTask.setTemplateDisplayName("DisplayA");
        dataFeedTask.setUniqueId(UUID.randomUUID().toString());
        datafeedTaskService.createDataFeedTask(customerSpace.toString(), dataFeedTask);
    }


    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testUpdate() {
        dataFeedTask = datafeedTaskService.getDataFeedTask(customerSpace.toString(), dataFeedTask.getUniqueId());
        Assert.assertNotNull(dataFeedTask);
        datafeedTaskService.setDataFeedTaskDelete(customerSpace.toString(), dataFeedTask.getPid(), Boolean.TRUE);
        dataFeedTask.setTemplateDisplayName("DisplayB");
        datafeedTaskService.updateDataFeedTask(customerSpace.toString(), dataFeedTask, true);
        dataFeedTask = datafeedTaskService.getDataFeedTask(customerSpace.toString(), dataFeedTask.getUniqueId());
        Assert.assertEquals(dataFeedTask.getTemplateDisplayName(), "DisplayB");
        datafeedTaskService.updateS3ImportStatus(customerSpace.toString(), dataFeedTask.getUniqueId(),
                DataFeedTask.S3ImportStatus.Active);
        dataFeedTask = datafeedTaskService.getDataFeedTask(customerSpace.toString(), dataFeedTask.getUniqueId());
        Assert.assertEquals(dataFeedTask.getS3ImportStatus(), DataFeedTask.S3ImportStatus.Active);

        datafeedTaskService.updateS3ImportStatus(customerSpace.toString(), dataFeedTask.getSource(),
                dataFeedTask.getFeedType(), DataFeedTask.S3ImportStatus.Pause);
        dataFeedTask = datafeedTaskService.getDataFeedTask(customerSpace.toString(), dataFeedTask.getUniqueId());
        Assert.assertEquals(dataFeedTask.getS3ImportStatus(), DataFeedTask.S3ImportStatus.Pause);
        datafeedTaskService.setDataFeedTaskS3ImportStatus(customerSpace.toString(), dataFeedTask.getPid(),
                DataFeedTask.S3ImportStatus.Active);
        dataFeedTask = datafeedTaskService.getDataFeedTask(customerSpace.toString(), dataFeedTask.getUniqueId());
        Assert.assertEquals(dataFeedTask.getS3ImportStatus(), DataFeedTask.S3ImportStatus.Active);

    }

    @Test(groups = "functional", dependsOnMethods = "testUpdate")
    public void testGetTemplateByPriority() {
        createS3ImportSystem();
        createMoreDataFeedTask("SYSTEM1");
        createMoreDataFeedTask("SYSTEM2");
        createMoreDataFeedTask("SYSTEM3");
        List<String> templates = datafeedTaskService.getTemplatesBySystemPriority(customerSpace.toString(),
                BusinessEntity.Account.name(), true);
        Assert.assertEquals(templates.size(), 3);
        Assert.assertEquals(templates.get(0), "SYSTEM3_importTable");
        templates = datafeedTaskService.getTemplatesBySystemPriority(customerSpace.toString(),
                BusinessEntity.Account.name(), false);
        Assert.assertEquals(templates.size(), 3);
        Assert.assertEquals(templates.get(0), "SYSTEM2_importTable");
    }
}

