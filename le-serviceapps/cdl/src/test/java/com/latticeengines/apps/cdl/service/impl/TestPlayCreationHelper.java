package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.apps.cdl.controller.PlayResourceDeploymentTestNG;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Component
public class TestPlayCreationHelper {

    private static final Logger log = LoggerFactory.getLogger(TestPlayCreationHelper.class);

    @Resource(name = "deploymentTestBed")
    private GlobalAuthDeploymentTestBed deploymentTestBed;

    @Inject
    private PlayResourceDeploymentTestNG playResourceDeploymentTestNG;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private SegmentService segmentService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private RatingProxy ratingProxy;

    private String tenantIdentifier;

    private Tenant tenant;

    private Play play;

    private RatingEngine ratingEngine;

    private PlayLaunch playLaunch;

    private MetadataSegment segment;

    public void setupTenantAndPopuldateData() {
        tenant = deploymentTestBed.bootstrapForProduct(LatticeProduct.CG);
        tenantIdentifier = tenant.getId();
        cdlTestDataService.populateData(tenantIdentifier);

        tenant = tenantEntityMgr.findByTenantId(tenantIdentifier);
        MultiTenantContext.setTenant(tenant);
    }

    public void setupTenantAndCreatePlay() throws Exception {
        setupTenantAndPopuldateData();

        playResourceDeploymentTestNG.setShouldSkipAutoTenantCreation(true);
        playResourceDeploymentTestNG.setTenant(tenant);

        Restriction accountRestriction = createAccountRestriction();
        Restriction contactRestriction = createContactRestriction();
        RatingRule ratingRule = createRatingRule();

        segment = playResourceDeploymentTestNG.createSegment(accountRestriction, contactRestriction);
        ratingEngine = playResourceDeploymentTestNG.createRatingEngine(segment, ratingRule);

        cdlTestDataService.mockRatingTableWithSingleEngine(tenant.getId(), ratingEngine.getId(), null);

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

    public RatingProxy initRatingProxy() throws NoSuchFieldException, IllegalAccessException {
        return ratingProxy;
    }

    public EntityProxy initEntityProxy() throws NoSuchFieldException, IllegalAccessException {
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
        ratingRule.setDefaultBucketName(RatingBucketName.C.name());
        return ratingRule;
    }

    private TreeMap<String, Map<String, Restriction>> populateBucketToRuleMap() {
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = new TreeMap<>();
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.A, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GTE_AND_LT, BusinessEntity.Account, "LDC_Name", "A", "G");
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.C, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GTE_AND_LTE, BusinessEntity.Account, "LDC_Name", "h", "n");
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.D, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GT_AND_LT, BusinessEntity.Account, "LDC_Name", "A", "O");
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.D, FrontEndQueryConstants.CONTACT_RESTRICTION, null,
                null, null, null, null);
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.F, FrontEndQueryConstants.ACCOUNT_RESTRICTION, null,
                null, null, null, null);
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.F, FrontEndQueryConstants.CONTACT_RESTRICTION, null,
                null, null, null, null);

        return bucketToRuleMap;
    }

    private void populateBucketInfo(TreeMap<String, Map<String, Restriction>> bucketToRuleMap,
            boolean createConcreteRestriction, RatingBucketName bucketName, String key, ComparisonType comparisonType,
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
}
