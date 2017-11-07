package com.latticeengines.pls.service.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.controller.PlayResourceDeploymentTestNG;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
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

    @Autowired
    private CDLTestDataService cdlTestDataService;

    private String tenantIdentifier;

    private Tenant tenant;

    private Play play;

    private RatingEngine ratingEngine;

    private PlayLaunch playLaunch;

    private MetadataSegment segment;

    public void setupTenant() {
        tenant = deploymentTestBed.bootstrapForProduct(LatticeProduct.CG);
        setupSecurityContext(tenant);
        tenantIdentifier = tenant.getId();
        cdlTestDataService.populateData(tenantIdentifier);

        tenant = tenantEntityMgr.findByTenantId(tenantIdentifier);
        MultiTenantContext.setTenant(tenant);
    }

    public void setupTenantAndCreatePlay() throws Exception {
        setupTenant();

        playResourceDeploymentTestNG.setShouldSkipAutoTenantCreation(true);
        playResourceDeploymentTestNG.setMainTestTenant(tenant);
        playResourceDeploymentTestNG.setup();

        Restriction accountRestriction = createAccountRestriction();
        Restriction contactRestriction = createContactRestriction();
        RatingRule ratingRule = createRatingRule();

        segment = playResourceDeploymentTestNG.createSegment(accountRestriction, contactRestriction);
        ratingEngine = playResourceDeploymentTestNG.createRatingEngine(segment, ratingRule);

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
                createBucketRestriction(2, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "CloudTechnologies_ContactCenterManagement");
        Restriction b4 = //
                createBucketRestriction(4, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "BusinessTechnologiesSsl");
        Restriction b5 = //
                createBucketRestriction(3, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "BusinessTechnologiesAnalytics");

        Restriction innerLogical1 = LogicalRestriction.builder()//
                .and(Arrays.asList(b1, b2, b3, b4, b5)).build();
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

    public RatingEngine getRatingEngine() {
        return ratingEngine;
    }

    public EntityProxy initEntityProxy() throws NoSuchFieldException, IllegalAccessException {

        EntityProxy entityProxy = new EntityProxy(null);

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
                ComparisonType.GTE_AND_LT, BusinessEntity.Account, "LDC_Name", "A", "G");
        populateBucketInfo(bucketToRuleMap, true, RuleBucketName.C, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GTE_AND_LTE, BusinessEntity.Account, "LDC_Name", "h", "n");
        populateBucketInfo(bucketToRuleMap, true, RuleBucketName.D, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GT_AND_LT, BusinessEntity.Account, "LDC_Name", "A", "O");
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
            AttributeLookup lhs = new AttributeLookup(entity, attrName);
            Bucket rhs = Bucket.rangeBkt(min, max);
            rhs.setComparisonType(comparisonType);
            info = new BucketRestriction(lhs, rhs);
        } else {
            info = LogicalRestriction.builder() //
                    .and(new ArrayList<>()).build();
        }

        bucketInfo.put(key, info);
    }

    private void setupSecurityContext(Tenant t) {
        SecurityContext securityContext = Mockito.mock(SecurityContext.class);
        TicketAuthenticationToken token = Mockito.mock(TicketAuthenticationToken.class);
        Session session = Mockito.mock(Session.class);
        Tenant tenant = Mockito.mock(Tenant.class);
        Mockito.when(session.getTenant()).thenReturn(tenant);
        Mockito.when(tenant.getId()).thenReturn(t.getId());
        Mockito.when(tenant.getPid()).thenReturn(t.getPid());
        Mockito.when(token.getSession()).thenReturn(session);
        Mockito.when(securityContext.getAuthentication()).thenReturn(token);
        SecurityContextHolder.setContext(securityContext);
    }
}
