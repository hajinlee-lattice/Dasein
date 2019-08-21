package com.latticeengines.apps.cdl.testframework;

import static com.latticeengines.domain.exposed.query.ComparisonType.EQUAL;
import static com.latticeengines.domain.exposed.query.ComparisonType.LESS_THAN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Listeners;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.testframework.service.impl.ContextResetTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class, ContextResetTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public class CDLFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CDLFunctionalTestNGBase.class);

    protected static final String SEGMENT_NAME = "CDLTestSegment";

    private static final String COMPANY_NAME = "CompanyName";

    private static final String STATE = "State";

    private static final String BUSINESS_TECHNOLOGIES_ANALYTICS = "BusinessTechnologiesAnalytics";

    private static final String BUSINESS_TECHNOLOGIES_SSL = "BusinessTechnologiesSsl";

    private static final String CLOUD_TECHNOLOGIES_CONTACT_CENTER_MANAGEMENT = "CloudTechnologies_ContactCenterManagement";

    private static final String PREMIUM_MARKETING_PRESCREEN = "PREMIUM_MARKETING_PRESCREEN";

    private static final String COMPOSITE_RISK_SCORE = "COMPOSITE_RISK_SCORE";

    @Resource(name = "globalAuthFunctionalTestBed")
    protected GlobalAuthFunctionalTestBed testBed;

    @Inject
    protected SegmentService segmentService;

    @Inject
    protected TenantEntityMgr tenantEntityMgr;

    @Inject
    protected TableEntityMgr tableEntityMgr;

    @Inject
    protected DataCollectionEntityMgr dataCollectionEntityMgr;

    protected Tenant mainTestTenant;
    protected MetadataSegment testSegment;
    protected List<String> accountAttributes;
    protected List<String> contactAttributes;
    protected String mainCustomerSpace;

    protected DataCollection dataCollection;
    protected String collectionName;

    protected void setupTestEnvironmentWithDummySegment() {
        accountAttributes = Arrays.asList(STATE, BUSINESS_TECHNOLOGIES_ANALYTICS, BUSINESS_TECHNOLOGIES_SSL,
                CLOUD_TECHNOLOGIES_CONTACT_CENTER_MANAGEMENT, PREMIUM_MARKETING_PRESCREEN, COMPOSITE_RISK_SCORE);
        contactAttributes = Arrays.asList(STATE, COMPANY_NAME);

        setupTestEnvironment();
        createDataCollection();
        testSegment = createMetadataSegment(SEGMENT_NAME);
        log.info(String.format("Created metadata segment with name %s", testSegment.getName()));
    }

    protected void setupTestEnvironmentWithDataCollection() {
        setupTestEnvironment();
        createDataCollection();
    }

    protected MetadataSegment createMetadataSegment(String segmentName) {
        Restriction accountRestriction = createAccountRestriction();
        Restriction contactRestriction = createContactRestriction();

        log.info("accountRestriction = " + JsonUtils.serialize(accountRestriction));
        log.info("contactRestriction = " + JsonUtils.serialize(contactRestriction));

        MetadataSegment metadataSegment = new MetadataSegment();
        metadataSegment.setDisplayName(segmentName);
        metadataSegment.setAccountRestriction(accountRestriction);
        metadataSegment.setContactRestriction(contactRestriction);

        metadataSegment = segmentService.createOrUpdateSegment(metadataSegment);
        MetadataSegment retrievedSegment = segmentService.findByName(metadataSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        return retrievedSegment;
    }

    protected void setupTestEnvironment() {
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        mainCustomerSpace = mainTestTenant.getId();
        MultiTenantContext.setTenant(mainTestTenant);
        testBed.switchToSuperAdmin();
    }

    private void createDataCollection() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(mainTestTenant.getId()));
        dataCollection = dataCollectionEntityMgr.createDefaultCollection();
        collectionName = dataCollection.getName();
    }

    protected void addTableToCollection(Table table, TableRoleInCollection role) {
        DataCollection.Version version = dataCollectionEntityMgr.findActiveVersion();
        if (tableEntityMgr.findByName(table.getName()) == null) {
            tableEntityMgr.create(table);
        }
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, table.getName(), role, version);
    }

    private Restriction createAccountRestriction() {
        Restriction b1 = //
                createBucketRestriction( //
                        BusinessEntity.Account, COMPOSITE_RISK_SCORE, EQUAL, 6);
        Restriction b2 = //
                createBucketRestriction( //
                        BusinessEntity.Account, PREMIUM_MARKETING_PRESCREEN, EQUAL, 1);
        Restriction b3 = //
                createBucketRestriction( //
                        BusinessEntity.Account, CLOUD_TECHNOLOGIES_CONTACT_CENTER_MANAGEMENT, LESS_THAN, 2);
        Restriction b4 = //
                createBucketRestriction( //
                        BusinessEntity.Account, BUSINESS_TECHNOLOGIES_SSL, LESS_THAN, 4);
        Restriction b5 = //
                createBucketRestriction( //
                        BusinessEntity.Account, BUSINESS_TECHNOLOGIES_ANALYTICS, EQUAL, 3);
        Restriction b6 = //
                createBucketRestriction( //
                        BusinessEntity.Account, STATE, EQUAL, "CA");

        Restriction innerLogical1 = LogicalRestriction.builder()//
                .and(Arrays.asList(b1, b2, b3, b4, b5, b6)).build();
        Restriction innerLogical2 = LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();

        return LogicalRestriction.builder() //
                .and(Arrays.asList(innerLogical1, innerLogical2)).build();
    }

    private Restriction createContactRestriction() {
        Restriction c1 = //
                createBucketRestriction(BusinessEntity.Contact, STATE, EQUAL, "CA");
        Restriction c2 = //
                createBucketRestriction(BusinessEntity.Contact, COMPANY_NAME, EQUAL, "TARLETON STATE UNIVERSITY");

        Restriction innerLogical1 = LogicalRestriction.builder()//
                .or(Arrays.asList(c1, c2)).build();
        Restriction innerLogical2 = LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();

        return LogicalRestriction.builder() //
                .and(Arrays.asList(innerLogical1, innerLogical2)).build();
    }

    protected Restriction createBucketRestriction(BusinessEntity entityType, String attrName, //
                                                  ComparisonType operator, Object... vals) {
        Bucket bucket = new Bucket();
        bucket.setComparisonType(operator);
        if (vals != null && vals.length > 0) {
            bucket.setValues(Arrays.asList(vals));
        }
        return new BucketRestriction(new AttributeLookup(entityType, attrName), bucket);
    }

    protected void createTable(String tableName) {
        Table newTable = new Table();
        newTable.setName(tableName);
        newTable.setDisplayName(tableName);
        newTable.setTenant(mainTestTenant);
        newTable.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(newTable);
    }

}
