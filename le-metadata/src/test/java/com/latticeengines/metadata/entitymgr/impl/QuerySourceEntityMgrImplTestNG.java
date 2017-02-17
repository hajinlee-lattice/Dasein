package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.QuerySource;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.entitymgr.QuerySourceEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import edu.emory.mathcs.backport.java.util.Collections;

public class QuerySourceEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {
    @Autowired
    private QuerySourceEntityMgr querySourceEntityMgr;
    private QuerySource defaultQuerySource;

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
        defaultQuerySource = querySourceEntityMgr.createQuerySource(Collections.singletonList(TABLE1), null, true);
        assertNotNull(defaultQuerySource);
        Table table = tableEntityMgr.findByName(TABLE1);
        assertTrue(table.getTags().contains(defaultQuerySource.getName()));
    }

    @Test(groups = "functional", dependsOnMethods = "createDefault")
    public void getDefault() {
        QuerySource retrieved = querySourceEntityMgr.getDefaultQuerySource();
        assertEquals(retrieved.getTables().size(), 1);
        assertEquals(retrieved.getTables().get(0).getName(), TABLE1);
        assertEquals(retrieved.getName(), defaultQuerySource.getName());
    }

    @Test(groups = "functional", dependsOnMethods = "getDefault")
    public void notVisibleInOtherTenant() {
        // TODO Validate that QuerySource is not available in other tenant
    }

    @Test(groups = "functional", dependsOnMethods = "notVisibleInOtherTenant")
    public void removeQuerySource() {
        // TODO Remove QuerySource and ensure tags are removed
    }
}
