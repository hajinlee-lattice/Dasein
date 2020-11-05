package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class DataFeedTaskEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    private DataFeed datafeed = new DataFeed();

    private DataFeedTask task = new DataFeedTask();

    private Table importTable = new Table();

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(mainCustomerSpace));
    }

    @Test(groups = "functional")
    public void create() {
        datafeed.setName("datafeed");
        datafeed.setDataCollection(dataCollection);

        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(MultiTenantContext.getTenant());
        Attribute a1 = new Attribute();
        a1.setName("a1");
        a1.setDisplayName(a1.getName());
        a1.setPhysicalDataType("string");
        importTable.addAttribute(a1);

        task.setDataFeed(datafeed);
        task.setActiveJob("Not specified");
        task.setEntity(SchemaInterpretation.Account.name());
        task.setSource("SFDC");
        task.setStatus(DataFeedTask.Status.Active);
        task.setSourceConfig("config");
        task.setImportTemplate(importTable);
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        task.setLastUpdated(new Date());
        task.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        datafeed.addTask(task);

        datafeedEntityMgr.create(datafeed);
        DataFeed dataFeed = datafeedEntityMgr.findByName("datafeed");
        dataFeed.setStatus(Status.Active);
        datafeedEntityMgr.update(dataFeed);
    }

}
