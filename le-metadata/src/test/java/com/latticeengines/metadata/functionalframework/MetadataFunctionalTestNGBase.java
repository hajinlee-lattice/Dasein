package com.latticeengines.metadata.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-metadata-context.xml" })
public class MetadataFunctionalTestNGBase  extends AbstractTestNGSpringContextTests {

    protected static final String CUSTOMERSPACE1 = "X.Y1.Z";
    protected static final String CUSTOMERSPACE2 = "X.Y2.Z";
    protected static final String TABLE1 = "AccountForCustomerSpace1";
    protected static final String TABLE2 = "AccountForCustomerSpace2";
    
    
    @Autowired
    protected TableEntityMgr tableEntityMgr;

    @Autowired
    protected TenantEntityMgr tenantEntityMgr;
    
    public void setup() {
        Tenant t1 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE1);
        if (t1 != null) {
            tenantEntityMgr.delete(t1);
        }
        Tenant t2 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE2);
        if (t2 != null) {
            tenantEntityMgr.delete(t2);
        }
        
        Tenant tenant1 = new Tenant();
        tenant1.setId(CUSTOMERSPACE1);
        tenant1.setName(CUSTOMERSPACE1);
        tenantEntityMgr.create(tenant1);
        Tenant tenant2 = new Tenant();
        tenant2.setId(CUSTOMERSPACE2);
        tenant2.setName(CUSTOMERSPACE2);
        tenantEntityMgr.create(tenant2);

        tableEntityMgr.create(createTable(tenant1, TABLE1));
        tableEntityMgr.create(createTable(tenant2, TABLE2));
    }
    
    private Table createTable(Tenant tenant, String tableName) {
        Table table = new Table();
        table.setTenant(tenant);
        table.setName(tableName);
        table.setDisplayName(tableName);
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

        Attribute pkAttr = new Attribute();
        pkAttr.setName("ID");
        pkAttr.setDisplayName("Id");
        pkAttr.setLength(10);
        pkAttr.setPrecision(10);
        pkAttr.setScale(10);
        pkAttr.setPhysicalDataType("XYZ");
        pkAttr.setLogicalDataType("Identity");

        Attribute lkAttr = new Attribute();
        lkAttr.setName("LID");
        lkAttr.setDisplayName("LastUpdatedDate");
        lkAttr.setLength(10);
        lkAttr.setPrecision(10);
        lkAttr.setScale(10);
        lkAttr.setPhysicalDataType("XYZ");
        lkAttr.setLogicalDataType("Date");
        
        table.addAttribute(pkAttr);
        table.addAttribute(lkAttr);
        
        return table;
    }

    private PrimaryKey createPrimaryKey(Tenant tenant) {
        PrimaryKey pk = new PrimaryKey();
        pk.setTenant(tenant);
        pk.setName("PK_ID");
        pk.setDisplayName("Primary Key for ID column");
        pk.addAttribute("ID");

        return pk;
    }

    private Extract createExtract(String name) {
        Extract e = new Extract();
        e.setName(name);
        e.setPath("/" + name);
        e.setExtractionTimestamp(System.currentTimeMillis());
        return e;
    }
    
    private LastModifiedKey createLastModifiedKey(Tenant tenant) {
        LastModifiedKey lk = new LastModifiedKey();
        lk.setTenant(tenant);
        lk.setName("LK_LUD");
        lk.setDisplayName("Last Modified Key for LastUpdatedDate column");
        lk.addAttribute("LID");
        
        return lk;
    }
    

}
