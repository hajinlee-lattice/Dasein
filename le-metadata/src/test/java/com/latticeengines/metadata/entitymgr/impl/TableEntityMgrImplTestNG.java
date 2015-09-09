package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public class TableEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {
    
    @Autowired
    private TableEntityMgr tableEntityMgr;
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @BeforeClass(groups = "functional")
    public void setup() {
        Tenant t = tenantEntityMgr.findByTenantId("tenant1");
        if (t != null) {
            tenantEntityMgr.delete(t);
        }
        
        Tenant tenant = new Tenant();
        tenant.setId("tenant1");
        tenant.setName("Tenant 1");
        tenantEntityMgr.create(tenant);
        
        Table table = new Table();
        table.setTenant(tenant);
        table.setName("source");
        table.setDisplayName("Source table");
        Extract e1 = new Extract();
        e1.setName("e1");
        e1.setPath("/e1");
        Extract e2 = new Extract();
        e2.setName("e2");
        e2.setPath("/e2");
        Extract e3 = new Extract();
        e3.setName("e3");
        e3.setPath("/e3");
        table.addExtract(e1);
        table.addExtract(e2);
        table.addExtract(e3);
        PrimaryKey pk = new PrimaryKey();
        Attribute pkAttr = new Attribute();
        table.addAttribute(pkAttr);
        pkAttr.setName("ID");
        pkAttr.setDisplayName("Id");
        pk.setName("PK_ID");
        pk.setDisplayName("Primary Key for ID column");
        pk.addAttribute(pkAttr);
        pk.setTenant(tenant);
        table.setPrimaryKey(pk);
        
        tableEntityMgr.create(table);
    }

    @Test(groups = "functional")
    public void findAll() {
        List<Table> tables = tableEntityMgr.findAll();
        
        assertEquals(tables.size(), 1);
    }

}
