package com.latticeengines.pls.service.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.controller.PlayResourceDeploymentTestNG;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Component
public class TestPlayCreationHelper {

    private static final Logger log = LoggerFactory.getLogger(TestPlayCreationHelper.class);

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Autowired
    private PlayResourceDeploymentTestNG playResourceDeploymentTestNG;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;
    @Autowired
    private SegmentService segmentService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    // this test tenant has account data loaded in CDL
    private String tenantIdentifier = "CDLTest_Lynn_0920.CDLTest_Lynn_0920.Production";

    private boolean tenantCleanupAllowed = false;

    private Tenant tenant;

    private Play play;

    private PlayLaunch playLaunch;

    private MetadataSegment segment;

    public void setupTenant() {
        setupTenant(tenantIdentifier);
    }

    public void setupTenant(String customTenantIdentifier) {
        if (StringUtils.isNoneBlank(customTenantIdentifier)) {
            tenantIdentifier = customTenantIdentifier;
        }

        tenant = tenantEntityMgr.findByTenantId(tenantIdentifier);
        if (tenant == null) {
            System.out.println("Creating new tenant: " + tenantIdentifier);
            tenantCleanupAllowed = true;
            tenant = deploymentTestBed.bootstrapForProduct(tenantIdentifier, LatticeProduct.LPA3);
        } else {
            deploymentTestBed.loginAD();
            deploymentTestBed.getTestTenants().add(tenant);
        }

        tenant = tenantEntityMgr.findByTenantId(tenantIdentifier);
        MultiTenantContext.setTenant(tenant);
    }

    public void setupTenantAndCreatePlay() throws Exception {
        setupTenantAndCreatePlay(tenantIdentifier);
    }

    public void setupTenantAndCreatePlay(String customTenantIdentifier) throws Exception {
        if (StringUtils.isNoneBlank(customTenantIdentifier)) {
            tenantIdentifier = customTenantIdentifier;
        }
        setupTenant(tenantIdentifier);

        playResourceDeploymentTestNG.setShouldSkipAutoTenantCreation(true);
        playResourceDeploymentTestNG.setMainTestTenant(tenant);
        playResourceDeploymentTestNG.setup();

        Restriction accountRestriction = createAccountRestriction();
        Restriction contactRestriction = createContactRestriction();
        RatingRule ratingRule = createRatingRule();

        segment = playResourceDeploymentTestNG.createSegment(accountRestriction, contactRestriction);
        playResourceDeploymentTestNG.createRatingEngine(segment, ratingRule);

        playResourceDeploymentTestNG.getCrud();
        playResourceDeploymentTestNG.createPlayLaunch();

        play = playResourceDeploymentTestNG.getPlay();
        playLaunch = playResourceDeploymentTestNG.getPlayLaunch();

        Assert.assertNotNull(play);
        Assert.assertNotNull(playLaunch);
    }

    private Restriction createAccountRestriction() {
        Restriction b1 = //
                createBucketRestriction(6, ComparisonType.EQUAL, //
                        BusinessEntity.Account, "COMPOSITE_RISK_SCORE");
        Restriction b2 = //
                createBucketRestriction(1, ComparisonType.EQUAL, //
                        BusinessEntity.Account, "PREMIUM_MARKETING_PRESCREEN");
        Restriction b3 = //
                createBucketRestriction("CALIFORNIA", ComparisonType.EQUAL, //
                        BusinessEntity.Account, "LDC_State");
        Restriction b4 = //
                createBucketRestriction(2, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "CloudTechnologies_ContactCenterManagement");
        Restriction b5 = //
                createBucketRestriction(4, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "BusinessTechnologiesSsl");
        Restriction b6 = //
                createBucketRestriction(3, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "BusinessTechnologiesAnalytics");

        Restriction innerLogical1 = LogicalRestriction.builder()//
                .and(Arrays.asList(b1, b2, b3, b4, b5, b6)).build();
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

    private Restriction createContactRestriction() {
        return LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();
    }

    public Tenant getTenant() {
        return tenant;
    }

    public Play getPlay() {
        return play;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    @SuppressWarnings("unchecked")
    public EntityProxy initEntityProxy() throws NoSuchFieldException, IllegalAccessException {
        Field propMapField = PropertyUtils.class.getDeclaredField("propertiesMap");
        propMapField.setAccessible(true);
        Map<String, String> propertiesMap = (Map<String, String>) propMapField.get(null);

        // temporary
        propertiesMap.put("common.microservice.url",
                "https://internal-private-lpi-b-282775961.us-east-1.elb.amazonaws.com");

        EntityProxy entityProxy = new EntityProxy();

        Field f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("initialWaitMsec");
        f1.setAccessible(true);
        f1.set(entityProxy, 1000L);

        f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("multiplier");
        f1.setAccessible(true);
        f1.set(entityProxy, 2D);

        f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("maxAttempts");
        f1.setAccessible(true);
        f1.set(entityProxy, 10);

        return entityProxy;
    }

    public void cleanupArtifacts() {
        try {
            log.info("Cleaning up play launch: " + playLaunch.getId());
            playResourceDeploymentTestNG.deletePlayLaunch(play.getName(), playLaunch.getId());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up play: " + play.getName());
            playResourceDeploymentTestNG.deletePlay(play.getName());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up rating engine: " + play.getRatingEngine().getId());
            ratingEngineEntityMgr.deleteById(play.getRatingEngine().getId());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up segment: " + segment.getName());
            segmentService.deleteSegmentByName(tenantIdentifier, segment.getName());
        } catch (Exception ex) {
            ignoreException(ex);
        }
    }

    private void ignoreException(Exception ex) {
        log.info("Could not cleanup artifact. Ignoring exception: ", ex);
    }

    public void cleanupTenant() {
        if (tenantCleanupAllowed) {
            deploymentTestBed.deleteTenant(tenant);
        }
    }

    private RatingRule createRatingRule() {
        RatingRule ratingRule = new RatingRule();
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = populateBucketToRuleMap();
        ratingRule.setBucketToRuleMap(bucketToRuleMap);
        ratingRule.setDefaultBucketName(RuleBucketName.C.name());
        return ratingRule;
    }

    private TreeMap<String, Map<String, Restriction>> populateBucketToRuleMap() {
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = new TreeMap<>();
        populateBucketInfo(bucketToRuleMap, true, RuleBucketName.A_MINUS, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.LESS_THAN, BusinessEntity.Account, "LDC_Name", "A", "G");
        populateBucketInfo(bucketToRuleMap, true, RuleBucketName.C, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.LESS_THAN, BusinessEntity.Account, "LDC_Name", "h", "n");
        populateBucketInfo(bucketToRuleMap, true, RuleBucketName.D, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.LESS_THAN, BusinessEntity.Account, "LDC_Name", "A", "O");
        populateBucketInfo(bucketToRuleMap, false, RuleBucketName.D, FrontEndQueryConstants.CONTACT_RESTRICTION, null,
                null, null, null, null);
        populateBucketInfo(bucketToRuleMap, false, RuleBucketName.F, FrontEndQueryConstants.ACCOUNT_RESTRICTION, null,
                null, null, null, null);
        populateBucketInfo(bucketToRuleMap, false, RuleBucketName.F, FrontEndQueryConstants.CONTACT_RESTRICTION, null,
                null, null, null, null);

        return bucketToRuleMap;
    }

    private void populateBucketInfo(TreeMap<String, Map<String, Restriction>> bucketToRuleMap,
            boolean createConcreteRestriction, RuleBucketName bucketName, String key, ComparisonType comparisonType,
            BusinessEntity entity, String attrName, Object min, Object max) {
        Map<String, Restriction> bucketInfo = bucketToRuleMap.get(bucketName.name());
        if (bucketInfo == null) {
            bucketInfo = new HashMap<>();
            bucketToRuleMap.put(bucketName.name(), bucketInfo);
        }

        Restriction info = null;

        if (createConcreteRestriction) {
            Lookup lhs = new AttributeLookup(entity, attrName);
            Lookup rhs = new RangeLookup(min, max);
            info = new ConcreteRestriction(false, lhs, comparisonType, rhs);
        } else {
            info = LogicalRestriction.builder() //
                    .and(new ArrayList<>()).build();
        }

        bucketInfo.put(key, info);
    }

}
