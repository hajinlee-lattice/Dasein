package com.latticeengines.metadata.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.service.impl.SetTenantAspect;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase.GetHttpStatusErrorHandler;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-metadata-context.xml" })
public class MetadataFunctionalTestNGBase  extends AbstractTestNGSpringContextTests {

    protected static final String CUSTOMERSPACE1 = "X.Y1.Z";
    protected static final String CUSTOMERSPACE2 = "X.Y2.Z";
    protected static final String TABLE1 = "AccountForCustomerSpace1";
    protected static final String TABLE2 = "AccountForCustomerSpace2";
    protected static final String TABLE3 = "AccountForCustomerSpace3";
    
    protected RestTemplate restTemplate = new RestTemplate();

    @Value("${metadata.test.functional.api:http://localhost:8080/}")
    private String hostPort;
    
    @Autowired
    protected TableEntityMgr tableEntityMgr;

    @Autowired
    protected TenantEntityMgr tenantEntityMgr;

    protected SecurityFunctionalTestNGBase securityTestBase = new SecurityFunctionalTestNGBase();

    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = securityTestBase.getAuthHeaderInterceptor();
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = securityTestBase.getMagicAuthHeaderInterceptor();
    protected GetHttpStatusErrorHandler statusErrorHandler = securityTestBase.getStatusErrorHandler();

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }


    public void setup() {
        Tenant t1 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE1);
        if (t1 != null) {
            tenantEntityMgr.delete(t1);

        }
        Tenant t2 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE2);
        if (t2 != null) {
            tenantEntityMgr.delete(t2);
        }

        Tenant tenant1 = createTenant(CUSTOMERSPACE1);
        tenantEntityMgr.create(tenant1);

        Tenant tenant2 = createTenant(CUSTOMERSPACE2);
        tenantEntityMgr.create(tenant2);

        new SetTenantAspect().setSecurityContext(tenant1);
        tableEntityMgr.create(createTable(tenant1, TABLE1));
        new SetTenantAspect().setSecurityContext(tenant2);
        tableEntityMgr.create(createTable(tenant2, TABLE2));
    }

    protected Tenant createTenant(String customerSpace) {
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace);
        tenant.setName(customerSpace);
        return tenant;
    }

    protected Table createTable(Tenant tenant, String tableName) {
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
        PrimaryKey pk = createPrimaryKey();
        LastModifiedKey lk = createLastModifiedKey();
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
        pkAttr.setPropertyValue("ApprovedUsage", "Model");

        Attribute lkAttr = new Attribute();
        lkAttr.setName("LID");
        lkAttr.setDisplayName("LastUpdatedDate");
        lkAttr.setLength(10);
        lkAttr.setPrecision(10);
        lkAttr.setScale(10);
        lkAttr.setPhysicalDataType("XYZ");
        lkAttr.setLogicalDataType("Date");
        lkAttr.setPropertyValue("ApprovedUsage", "Model");

        table.addAttribute(pkAttr);
        table.addAttribute(lkAttr);

        return table;
    }

    protected PrimaryKey createPrimaryKey() {
        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.setDisplayName("Primary Key for ID column");
        pk.addAttribute("ID");

        return pk;
    }

    protected Extract createExtract(String name) {
        Extract e = new Extract();
        e.setName(name);
        e.setPath("/" + name);
        e.setExtractionTimestamp(System.currentTimeMillis());
        return e;
    }

    protected LastModifiedKey createLastModifiedKey() {
        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LUD");
        lk.setDisplayName("Last Modified Key for LastUpdatedDate column");
        lk.addAttribute("LID");
        
        return lk;
    }
    

}
