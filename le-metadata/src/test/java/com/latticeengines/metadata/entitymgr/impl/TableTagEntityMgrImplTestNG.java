package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.entitymgr.TableTagEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class TableTagEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private TableTagEntityMgr tableTagEntityMgr;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace2));
    }

    @Test(groups = "functional")
    public void getTablesForTag() {
        List<Table> tables = tableTagEntityMgr.getTablesForTag("TAG1");
        assertEquals(tables.size(), 1);
    }

    @Test(groups = "functional")
    public void testSerialization() {
        List<Table> tables = tableTagEntityMgr.getTablesForTag("TAG1");
        String json = JsonUtils.serialize(tables);
        assertNotNull(json);
    }
}
