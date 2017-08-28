package com.latticeengines.pls.entitymanager.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.entitymanager.RuleBasedModelEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class RuleBasedModelEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelEntityMgrImplTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final Restriction A1 = bucket(Account, 1);
    private static final Restriction A2 = bucket(Account, 2);
    private static final Restriction C1 = bucket(Contact, 1);
    private static final Restriction C2 = bucket(Contact, 2);
    private static final Restriction C3 = bucket(Contact, 3);

    @Autowired
    private SegmentService segmentService;

    @Autowired
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private TenantService tenantService;

    private RatingEngine ratingEngine;
    private String ratingEngineId;

    private MetadataSegment segment;

    private RuleBasedModel ruleBasedModel;
    private Tenant tenant;
    private String ruleBasedModelId;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithGATenants(1);
        tenant = testBed.getTestTenants().get(0);
        MultiTenantContext.setTenant(tenant);

        segment = new MetadataSegment();
        segment.setFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentService.findByName(CustomerSpace.parse(tenant.getId()).toString(),
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));
        ratingEngine = new RatingEngine();
        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);

        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine,
                tenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine);
    }

    @Test(groups = "functional")
    public void testBasicOperations() {
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNotNull(createdRatingEngine);
        List<RuleBasedModel> ruleBasedModelList = ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
        Assert.assertNotNull(ruleBasedModelList);
        Assert.assertEquals(1, ruleBasedModelList.size());
        ruleBasedModel = ruleBasedModelList.get(0);
        assertDefaultRuleBasedModel(ruleBasedModel);
        ruleBasedModelId = ruleBasedModel.getId();

        ruleBasedModel.setRatingRule(generateRatingRule());
        ruleBasedModelEntityMgr.createOrUpdateRuleBasedModel(ruleBasedModel, ratingEngineId);
        ruleBasedModelList = ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
        Assert.assertNotNull(ruleBasedModelList);
        Assert.assertEquals(1, ruleBasedModelList.size());
        assertUpdatedRuleBasedModel(ruleBasedModelList.get(0));
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
    }

    private void assertDefaultRuleBasedModel(RuleBasedModel ruleBasedModel) {
        Assert.assertNotNull(ruleBasedModel);
        Assert.assertNotNull(ruleBasedModel.getId());
        Assert.assertEquals(1, ruleBasedModel.getIteration());
        Assert.assertNotNull(ruleBasedModel.getRatingEngine());
        Assert.assertEquals(ruleBasedModel.getRatingEngine().getId(), ratingEngineId);
        Assert.assertNotNull(ruleBasedModel.getRatingRule());
        Assert.assertEquals(RatingRule.DEFAULT_BUCKET_NAME, ruleBasedModel.getRatingRule().getDefaultBucketName());
    }

    private void assertUpdatedRuleBasedModel(RuleBasedModel ruleBasedModel) {
        Assert.assertNotNull(ruleBasedModel);
        Assert.assertEquals(ruleBasedModel.getId(), ruleBasedModelId);
        Assert.assertEquals(1, ruleBasedModel.getIteration());
        Assert.assertNotNull(ruleBasedModel.getRatingEngine());
        Assert.assertEquals(ruleBasedModel.getRatingEngine().getId(), ratingEngineId);
        Assert.assertNotNull(ruleBasedModel.getRatingRule());
        Assert.assertEquals(RuleBucketName.D.getName(), ruleBasedModel.getRatingRule().getDefaultBucketName());
    }

    private RatingRule generateRatingRule() {
        RatingRule rr = new RatingRule();
        rr.setDefaultBucketName(RuleBucketName.D.getName());
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = new TreeMap<>();
        Map<String, Restriction> Amap = new HashMap<>();
        Map<String, Restriction> Bmap = new HashMap<>();
        Map<String, Restriction> Dmap = new HashMap<>();
        bucketToRuleMap.put(RuleBucketName.A.getName(), Amap);
        bucketToRuleMap.put(RuleBucketName.D.getName(), Dmap);
        bucketToRuleMap.put(RuleBucketName.B.getName(), Bmap);
        Amap.put(RatingRule.CONTACT_RULE, Restriction.builder().and(Collections.emptyList()).build());
        Bmap.put(RatingRule.CONTACT_RULE, Restriction.builder().and(Collections.emptyList()).build());
        Dmap.put(RatingRule.CONTACT_RULE, Restriction.builder().and(Collections.emptyList()).build());
        Restriction r1 = and(A1, C1, A2, C2);
        Restriction r2 = and(and(A1, A2), and(C1, C2));
        Restriction r3 = or(and(A1, A2, C1), or(A1, C2, C3));
        Amap.put(RatingRule.ACCOUNT_RULE, r1);
        Dmap.put(RatingRule.ACCOUNT_RULE, r2);
        Bmap.put(RatingRule.ACCOUNT_RULE, r3);
        rr.setBucketToRuleMap(bucketToRuleMap);
        log.info(String.format("Returned RatingRule is %s", rr.toString()));
        return rr;
    }

    private static BucketRestriction bucket(BusinessEntity entity, int idx) {
        return BucketRestriction.from(Restriction.builder() //
                .let(entity, entity.name().substring(0, 1)) //
                .eq(String.valueOf(idx)) //
                .build());
    }

    private static Restriction and(Restriction... restrictions) {
        return Restriction.builder().and(restrictions).build();
    }

    private static Restriction or(Restriction... restrictions) {
        return Restriction.builder().or(restrictions).build();
    }

}
