package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataFeedEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    private static final String DATA_FEED_NAME = "datafeed";

    private DataFeed datafeed = new DataFeed();

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        datafeedEntityMgr.delete(datafeed);
        super.cleanup();
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
    }

    @Test(groups = "functional")
    public void create() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setName("DATA_COLLECTION_NAME");
        Table table = new Table();
        table.setName(TABLE1);
        dataCollection.setTables(Collections.singletonList(table));
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollectionEntityMgr.createDataCollection(dataCollection);

        datafeed.setName(DATA_FEED_NAME);
        datafeed.setStatus(Status.Active);
        datafeed.setActiveExecution(0L);
        datafeed.setDataCollection(dataCollection);
        dataCollection.addDataFeed(datafeed);
        datafeedEntityMgr.create(datafeed);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataFeed retrieved = datafeedEntityMgr.findByField("name", DATA_FEED_NAME);
        assertEquals(retrieved.getName(), datafeed.getName());
        assertEquals(retrieved.getActiveExecution(), datafeed.getActiveExecution());
    }

}
