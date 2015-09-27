package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
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
        List<Table> tables = tableEntityMgr.findAll();
        
        for (Table table : tables) {
            tableEntityMgr.delete(table);
        }
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
        Extract e1 = createExtract("e1");
        Extract e2 = createExtract("e2");
        Extract e3 = createExtract("e3");
        table.addExtract(e1);
        table.addExtract(e2);
        table.addExtract(e3);
        PrimaryKey pk = createPrimaryKey(tenant);
        LastModifiedKey lk = createLastModifiedKey(tenant);
        table.setPrimaryKey(pk);
        table.setLastModifiedKey(lk);
        table.addAttribute(pk.getAttributes().get(0));
        table.addAttribute(lk.getAttributes().get(0));
        
        tableEntityMgr.create(table);
    }
    
    private LastModifiedKey createLastModifiedKey(Tenant tenant) {
        LastModifiedKey lk = new LastModifiedKey();
        lk.setTenant(tenant);
        Attribute lkAttr = new Attribute();
        lkAttr.setName("LID");
        lkAttr.setDisplayName("LastUpdatedDate");
        lkAttr.setLength(10);
        lkAttr.setPrecision(10);
        lkAttr.setScale(10);
        lkAttr.setPhysicalDataType("XYZ");
        lkAttr.setLogicalDataType("Date");
        lk.setName("LK_LUD");
        lk.setDisplayName("Last Modified Key for LastUpdatedDate column");
        lk.addAttribute(lkAttr);
        
        return lk;
    }
    
    private PrimaryKey createPrimaryKey(Tenant tenant) {
        PrimaryKey pk = new PrimaryKey();
        pk.setTenant(tenant);
        Attribute pkAttr = new Attribute();
        pkAttr.setName("ID");
        pkAttr.setDisplayName("Id");
        pkAttr.setLength(10);
        pkAttr.setPrecision(10);
        pkAttr.setScale(10);
        pkAttr.setPhysicalDataType("XYZ");
        pkAttr.setLogicalDataType("Identity");
        pk.setName("PK_ID");
        pk.setDisplayName("Primary Key for ID column");
        pk.addAttribute(pkAttr);

        return pk;
    }

    private Extract createExtract(String name) {
        Extract e = new Extract();
        e.setName(name);
        e.setPath("/" + name);
        e.setExtractionTimestamp(System.currentTimeMillis());
        return e;
    }

    @Test(groups = "functional")
    public void findAll() {
        List<Table> tables = tableEntityMgr.findAll();
        
        assertEquals(tables.size(), 1);
    }

}
