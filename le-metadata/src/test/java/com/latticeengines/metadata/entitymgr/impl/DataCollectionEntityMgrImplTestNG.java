package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import edu.emory.mathcs.backport.java.util.Collections;

public class DataCollectionEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {
    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;
    private DataCollection defaultDataCollection;

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
    public void createDefault() {
        defaultDataCollection = dataCollectionEntityMgr.createDataCollection(Collections.singletonList(TABLE1), null,
                true);
        assertNotNull(defaultDataCollection);
        Table table = tableEntityMgr.findByName(TABLE1);
        assertTrue(table.getTags().contains(defaultDataCollection.getName()));
    }

    @Test(groups = "functional", dependsOnMethods = "createDefault")
    public void getDefault() {
        DataCollection retrieved = dataCollectionEntityMgr.getDefaultDataCollection();
        assertEquals(retrieved.getTables().size(), 1);
        assertEquals(retrieved.getTables().get(0).getName(), TABLE1);
        assertEquals(retrieved.getName(), defaultDataCollection.getName());
    }

    @Test(groups = "functional", dependsOnMethods = "getDefault")
    public void notVisibleInOtherTenant() {
        // TODO Validate that DataCollection is not available in other tenant
    }

    @Test(groups = "functional", dependsOnMethods = "notVisibleInOtherTenant")
    public void removeQuerySource() {
        // TODO Remove DataCollection and ensure tags are removed
    }
}
