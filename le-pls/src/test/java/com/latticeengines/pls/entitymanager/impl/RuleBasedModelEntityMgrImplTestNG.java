package com.latticeengines.pls.entitymanager.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
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

    private static final String ATTR1 = "Employ Number";
    private static final String ATTR2 = "Revenue";
    private static final String ATTR3 = "Has Cisco WebEx";

    @Autowired
    private SegmentService segmentService;

    @Autowired
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

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
        segment.setAccountRestriction(new FrontEndRestriction());
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
        // default ruleBasedModel should have been persisted with rating engine
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNotNull(createdRatingEngine);
        List<RuleBasedModel> ruleBasedModelList = ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
        Assert.assertNotNull(ruleBasedModelList);
        Assert.assertEquals(ruleBasedModelList.size(), 1);
        ruleBasedModel = ruleBasedModelList.get(0);
        assertDefaultRuleBasedModel(ruleBasedModel);
        ruleBasedModelId = ruleBasedModel.getId();

        // update ruleBasedModel by updating its selected attributes and rules
        ruleBasedModel.setRatingRule(generateRatingRule());
        ruleBasedModel.setSelectedAttributes(generateSeletedAttributes());
        ruleBasedModelEntityMgr.createOrUpdateRuleBasedModel(ruleBasedModel, ratingEngineId);
        ruleBasedModelList = ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
        Assert.assertNotNull(ruleBasedModelList);
        Assert.assertEquals(ruleBasedModelList.size(), 1);
        assertUpdatedRuleBasedModel(ruleBasedModelList.get(0));
    }

    private void assertDefaultRuleBasedModel(RuleBasedModel ruleBasedModel) {
        Assert.assertNotNull(ruleBasedModel);
        Assert.assertNotNull(ruleBasedModel.getId());
        Assert.assertEquals(1, ruleBasedModel.getIteration());
        Assert.assertNotNull(ruleBasedModel.getRatingEngine());
        Assert.assertEquals(ruleBasedModel.getRatingEngine().getId(), ratingEngineId);
        Assert.assertNotNull(ruleBasedModel.getRatingRule());
        Assert.assertEquals(ruleBasedModel.getRatingRule().getDefaultBucketName(), RatingRule.DEFAULT_BUCKET_NAME);
        Assert.assertNull(ruleBasedModel.getSelectedAttributes());
    }

    private void assertUpdatedRuleBasedModel(RuleBasedModel ruleBasedModel) {
        Assert.assertNotNull(ruleBasedModel);
        Assert.assertEquals(ruleBasedModel.getId(), ruleBasedModelId);
        Assert.assertEquals(1, ruleBasedModel.getIteration());
        Assert.assertNotNull(ruleBasedModel.getRatingEngine());
        Assert.assertEquals(ruleBasedModel.getRatingEngine().getId(), ratingEngineId);
        Assert.assertNotNull(ruleBasedModel.getRatingRule());
        Assert.assertEquals(ruleBasedModel.getRatingRule().getDefaultBucketName(), RuleBucketName.D.getName());

        Assert.assertNotNull(ruleBasedModel.getSelectedAttributes());
        Assert.assertTrue(ruleBasedModel.getSelectedAttributes().contains(ATTR1));
        Assert.assertTrue(ruleBasedModel.getSelectedAttributes().contains(ATTR2));
        Assert.assertTrue(ruleBasedModel.getSelectedAttributes().contains(ATTR3));
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

    private List<String> generateSeletedAttributes() {
        List<String> selectedAttributes = new ArrayList<>();
        selectedAttributes.add(ATTR1);
        selectedAttributes.add(ATTR2);
        selectedAttributes.add(ATTR3);
        return selectedAttributes;
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
