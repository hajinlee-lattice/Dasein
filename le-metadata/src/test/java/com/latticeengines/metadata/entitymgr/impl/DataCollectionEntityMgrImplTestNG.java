package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import edu.emory.mathcs.backport.java.util.Collections;

public class DataCollectionEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {
    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;
    private DataCollection dataCollection;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(CUSTOMERSPACE1));
    }

    @Test(groups = "functional")
    @SuppressWarnings("unchecked")
    public void create() {
        DataCollection collection = new DataCollection();
        Table table = new Table();
        table.setName(TABLE1);
        collection.setTables(Collections.singletonList(table));
        collection.setType(DataCollectionType.Segmentation);
        dataCollection = dataCollectionEntityMgr.createDataCollection(collection);
        assertNotNull(dataCollection);
        table = tableEntityMgr.findByName(TABLE1);
        assertTrue(table.getTags().contains(dataCollection.getName()));
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataCollection retrieved = dataCollectionEntityMgr.getDataCollection(DataCollectionType.Segmentation);
        assertEquals(retrieved.getTables().size(), 1);
        assertEquals(retrieved.getTables().get(0).getName(), TABLE1);
        assertEquals(retrieved.getName(), dataCollection.getName());
    }

    @Test(groups = "functional", dependsOnMethods = "retrieve")
    public void retrieveByName() {
        DataCollection retrieved = dataCollectionEntityMgr.getDataCollection(dataCollection.getName());
        assertEquals(retrieved.getTables().size(), 1);
        assertEquals(retrieved.getTables().get(0).getName(), TABLE1);
        assertEquals(retrieved.getName(), dataCollection.getName());
    }
}
