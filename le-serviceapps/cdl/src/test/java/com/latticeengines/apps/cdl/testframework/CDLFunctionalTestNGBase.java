package com.latticeengines.apps.cdl.testframework;

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

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
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
    private GlobalAuthFunctionalTestBed testBed;

    @Inject
    private SegmentService segmentService;

    protected Tenant mainTestTenant;
    protected MetadataSegment testSegment;
    protected List<String> accountAttributes;
    protected List<String> contactAttributes;

    protected void setupTestEnvironmentWithDummySegment() {
        accountAttributes = Arrays.asList(STATE, BUSINESS_TECHNOLOGIES_ANALYTICS, BUSINESS_TECHNOLOGIES_SSL,
                CLOUD_TECHNOLOGIES_CONTACT_CENTER_MANAGEMENT, PREMIUM_MARKETING_PRESCREEN, COMPOSITE_RISK_SCORE);
        contactAttributes = Arrays.asList(STATE, COMPANY_NAME);

        setupTestEnvironment();
        testSegment = createMetadataSegment(SEGMENT_NAME);
        log.info(String.format("Created metadata segment with name %s", testSegment.getName()));
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

        metadataSegment = segmentService.createOrUpdateSegment(CustomerSpace.parse(mainTestTenant.getId()).toString(),
                metadataSegment);
        MetadataSegment retrievedSegment = segmentService
                .findByName(CustomerSpace.parse(mainTestTenant.getId()).toString(), metadataSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        return retrievedSegment;
    }

    private void setupTestEnvironment() {
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        testBed.switchToSuperAdmin();
    }

    private Restriction createAccountRestriction() {
        Restriction b1 = //
                createBucketRestriction(6, ComparisonType.EQUAL, //
                        BusinessEntity.Account, COMPOSITE_RISK_SCORE);
        Restriction b2 = //
                createBucketRestriction(1, ComparisonType.EQUAL, //
                        BusinessEntity.Account, PREMIUM_MARKETING_PRESCREEN);
        Restriction b3 = //
                createBucketRestriction(2, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, CLOUD_TECHNOLOGIES_CONTACT_CENTER_MANAGEMENT);
        Restriction b4 = //
                createBucketRestriction(4, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, BUSINESS_TECHNOLOGIES_SSL);
        Restriction b5 = //
                createBucketRestriction(3, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, BUSINESS_TECHNOLOGIES_ANALYTICS);
        Restriction b6 = //
                createBucketRestriction("CA", ComparisonType.EQUAL, //
                        BusinessEntity.Account, STATE);

        Restriction innerLogical1 = LogicalRestriction.builder()//
                .and(Arrays.asList(b1, b2, b3, b4, b5, b6)).build();
        Restriction innerLogical2 = LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();

        return LogicalRestriction.builder() //
                .and(Arrays.asList(innerLogical1, innerLogical2)).build();
    }

    private Restriction createContactRestriction() {
        Restriction c1 = //
                createBucketRestriction("CA", ComparisonType.EQUAL, //
                        BusinessEntity.Contact, STATE);
        Restriction c2 = //
                createBucketRestriction("TARLETON STATE UNIVERSITY", ComparisonType.EQUAL, //
                        BusinessEntity.Contact, COMPANY_NAME);

        Restriction innerLogical1 = LogicalRestriction.builder()//
                .or(Arrays.asList(c1, c2)).build();
        Restriction innerLogical2 = LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();

        return LogicalRestriction.builder() //
                .and(Arrays.asList(innerLogical1, innerLogical2)).build();
    }

    private Restriction createBucketRestriction(Object val, ComparisonType comparisonType, BusinessEntity entityType,
            String attrName) {
        Bucket bucket = null;

        if (comparisonType == ComparisonType.EQUAL) {
            bucket = Bucket.valueBkt(comparisonType, Arrays.asList(val));
        } else if (comparisonType == ComparisonType.LESS_THAN) {
            bucket = Bucket.rangeBkt(null, val);
        }
        return new BucketRestriction(new AttributeLookup(entityType, attrName), bucket);
    }

}
