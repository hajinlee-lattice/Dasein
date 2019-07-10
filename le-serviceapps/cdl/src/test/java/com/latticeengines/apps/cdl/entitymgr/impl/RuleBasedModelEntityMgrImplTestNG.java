package com.latticeengines.apps.cdl.entitymgr.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;

public class RuleBasedModelEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelEntityMgrImplTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
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
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    private RatingEngine ratingEngine;
    private String ratingEngineId;

    private RuleBasedModel ruleBasedModel;
    private String ruleBasedModelId;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
        ratingEngine = new RatingEngine();
        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        ratingEngine.setSegment(testSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setId(UUID.randomUUID().toString());

        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngine);
        Assert.assertNotNull(createdRatingEngine);
        ratingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine);
        ActionContext.remove();
    }

    @AfterClass(groups = "functional")
    public void cleanupActionContext() {
        ActionContext.remove();
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
        ruleBasedModelEntityMgr.updateRuleBasedModel(ruleBasedModel, ruleBasedModelEntityMgr.findById(ruleBasedModelId),
                ratingEngineId);
        validateNonAction();

        // set Rating Engine to active to mimic the fact this Rating Engine is
        // complete.
        ratingEngine.setScoringIteration(createdRatingEngine.getLatestIteration());
        ratingEngineEntityMgr.updateRatingEngine(ratingEngine, ratingEngineEntityMgr.findById(ratingEngine.getId()),
                false);
        ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        ratingEngineEntityMgr.updateRatingEngine(ratingEngine, ratingEngineEntityMgr.findById(ratingEngine.getId()),
                false);

        ruleBasedModel.setRatingRule(generateRatingRule());
        ruleBasedModel.setSelectedAttributes(generateSeletedAttributes());
        ruleBasedModelEntityMgr.updateRuleBasedModel(ruleBasedModel, ruleBasedModelEntityMgr.findById(ruleBasedModelId),
                ratingEngineId);
        validateActionContext(ruleBasedModel);
        ruleBasedModelList = ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
        Assert.assertNotNull(ruleBasedModelList);
        Assert.assertEquals(ruleBasedModelList.size(), 1);
        assertUpdatedRuleBasedModel(ruleBasedModelList.get(0));
    }

    private void validateNonAction() {
        Action action = ActionContext.getAction();
        Assert.assertNull(action);
    }

    private void validateActionContext(RatingModel ratingModel) {
        Action action = ActionContext.getAction();
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);
        Assert.assertNull(action.getActionInitiator());
        Assert.assertTrue(action.getActionConfiguration() instanceof RatingEngineActionConfiguration);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getSubType(),
                RatingEngineActionConfiguration.SubType.RULE_MODEL_BUCKET_CHANGE);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getRatingEngineId(),
                ratingEngineId);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getModelId(),
                ratingModel.getId());
    }

    private void assertDefaultRuleBasedModel(RuleBasedModel ruleBasedModel) {
        Assert.assertNotNull(ruleBasedModel);
        Assert.assertNotNull(ruleBasedModel.getId());
        Assert.assertEquals(1, ruleBasedModel.getIteration());
        Assert.assertNotNull(ruleBasedModel.getRatingEngine());
        Assert.assertEquals(ruleBasedModel.getRatingEngine().getId(), ratingEngineId);
        Assert.assertNotNull(ruleBasedModel.getRatingRule());
        Assert.assertEquals(ruleBasedModel.getRatingRule().getDefaultBucketName(), RatingRule.DEFAULT_BUCKET_NAME);
        Assert.assertNotNull(ruleBasedModel.getSelectedAttributes());
    }

    private void assertUpdatedRuleBasedModel(RuleBasedModel ruleBasedModel) {
        Assert.assertNotNull(ruleBasedModel);
        Assert.assertEquals(ruleBasedModel.getId(), ruleBasedModelId);
        Assert.assertEquals(1, ruleBasedModel.getIteration());
        Assert.assertNotNull(ruleBasedModel.getRatingEngine());
        Assert.assertEquals(ruleBasedModel.getRatingEngine().getId(), ratingEngineId);
        Assert.assertNotNull(ruleBasedModel.getRatingRule());
        Assert.assertEquals(ruleBasedModel.getRatingRule().getDefaultBucketName(), RatingBucketName.D.getName());

        Assert.assertNotNull(ruleBasedModel.getSelectedAttributes());
        Assert.assertTrue(ruleBasedModel.getSelectedAttributes().contains(ATTR1));
        Assert.assertTrue(ruleBasedModel.getSelectedAttributes().contains(ATTR2));
        Assert.assertTrue(ruleBasedModel.getSelectedAttributes().contains(ATTR3));
    }

    private RatingRule generateRatingRule() {
        RatingRule rr = new RatingRule();
        rr.setDefaultBucketName(RatingBucketName.D.getName());
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = new TreeMap<>();
        Map<String, Restriction> Amap = new HashMap<>();
        Map<String, Restriction> Bmap = new HashMap<>();
        Map<String, Restriction> Dmap = new HashMap<>();
        bucketToRuleMap.put(RatingBucketName.A.getName(), Amap);
        bucketToRuleMap.put(RatingBucketName.D.getName(), Dmap);
        bucketToRuleMap.put(RatingBucketName.B.getName(), Bmap);
        Amap.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                Restriction.builder().and(Collections.emptyList()).build());
        Bmap.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                Restriction.builder().and(Collections.emptyList()).build());
        Dmap.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                Restriction.builder().and(Collections.emptyList()).build());
        Restriction r1 = and(A1, C1, A2, C2);
        Restriction r2 = and(and(A1, A2), and(C1, C2));
        Restriction r3 = or(and(A1, A2, C1), or(A1, C2, C3));
        Amap.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, r1);
        Dmap.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, r2);
        Bmap.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, r3);
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
        Bucket bucket = Bucket.valueBkt(String.valueOf(idx));
        AttributeLookup attrLookup = new AttributeLookup(entity, entity.name().substring(0, 1));
        return new BucketRestriction(attrLookup, bucket);
    }

    private static Restriction and(Restriction... restrictions) {
        return Restriction.builder().and(restrictions).build();
    }

    private static Restriction or(Restriction... restrictions) {
        return Restriction.builder().or(restrictions).build();
    }

}
