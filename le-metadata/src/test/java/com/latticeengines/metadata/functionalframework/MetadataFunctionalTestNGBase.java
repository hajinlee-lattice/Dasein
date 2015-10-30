package com.latticeengines.metadata.functionalframework;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
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
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase.GetHttpStatusErrorHandler;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-metadata-context.xml", "classpath:metadata-aspects-context.xml" })
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
    
    @Autowired
    protected TableTypeHolder tableTypeHolder;

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

        // Tenant1, Type=DATATABLE
        Table tbl = createTable(tenant1, TABLE1);
        createTableByRestCall(tenant1, tbl, false);
        // Tenant1, Type=IMPORTTABLE
        createTableByRestCall(tenant1, tbl, true);

        // Tenant2, Type=DATATABLE
        tbl = createTable(tenant2, TABLE2);
        createTableByRestCall(tenant2, tbl, false);
        // Tenant2, Type=IMPORTTABLE
        createTableByRestCall(tenant2, tbl, true);
    }
    
    private void createTableByRestCall(Tenant tenant, Table table, boolean isImport) {
        String urlType = "tables";
        if (isImport) {
            urlType = "importtables";
        }
        
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s",
                getRestAPIHostPort(), table.getTenant().getId(), urlType, table.getName());
        restTemplate.postForLocation(url, table);

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
        pkAttr.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);

        Attribute lkAttr = new Attribute();
        lkAttr.setName("LID");
        lkAttr.setDisplayName("LastUpdatedDate");
        lkAttr.setLength(20);
        lkAttr.setPrecision(20);
        lkAttr.setScale(20);
        lkAttr.setPhysicalDataType("ABC");
        lkAttr.setLogicalDataType("Date");
        lkAttr.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);

        Attribute spamIndicator = new Attribute();
        spamIndicator.setName("SPAM_INDICATOR");
        spamIndicator.setDisplayName("SpamIndicator");
        spamIndicator.setLength(20);
        spamIndicator.setPrecision(-1);
        spamIndicator.setScale(-1);
        spamIndicator.setPhysicalDataType("Boolean");
        spamIndicator.setLogicalDataType("Boolean");
        spamIndicator.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        
        Attribute activeRetirementParticipants = new Attribute();
        activeRetirementParticipants.setName("ActiveRetirementParticipants");
        activeRetirementParticipants.setDisplayName("Active Retirement Plan Participants");
        activeRetirementParticipants.setDescription("Number of active retirement plan participants");
        activeRetirementParticipants.setLength(5);
        activeRetirementParticipants.setPrecision(0);
        activeRetirementParticipants.setScale(0);
        activeRetirementParticipants.setPhysicalDataType(Schema.Type.INT.toString());
        activeRetirementParticipants.setLogicalDataType("Integer");
        activeRetirementParticipants.setApprovedUsage(ModelingMetadata.MODEL_APPROVED_USAGE);
        activeRetirementParticipants.setCategory("Firmographics");
        activeRetirementParticipants.setDataType("Int");
        activeRetirementParticipants.setFundamentalType("numeric");
        activeRetirementParticipants.setStatisticalType("ratio");
        activeRetirementParticipants.setTags(ModelingMetadata.EXTERNAL_TAG);
        activeRetirementParticipants.setDataSource("DerivedColumns");

        table.addAttribute(pkAttr);
        table.addAttribute(lkAttr);
        table.addAttribute(spamIndicator);
        table.addAttribute(activeRetirementParticipants);

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
        lk.setLastModifiedTimestamp(1000000000000L);
        return lk;
    }
    

}
