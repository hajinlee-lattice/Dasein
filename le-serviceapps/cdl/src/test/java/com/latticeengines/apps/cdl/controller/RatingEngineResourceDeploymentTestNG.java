package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelWorkflowType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;

public class RatingEngineResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResourceDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME_1 = "Rating Engine 1";
    private static final String RATING_ENGINE_NOTE_1 = "This is a Rating Engine that covers North America market";
    private static final String RATING_ENGINE_NEW_NOTE = "This is a Rating Engine that covers East Asia market";
    @SuppressWarnings("unused")
    private static final String RATING_ENGINE_NAME_2 = "Rating Engine 2";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final String ATTR1 = "Employ Number";
    private static final String ATTR2 = "Revenue";
    private static final String ATTR3 = "Has Cisco WebEx";

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private RatingEngine re1;
    private RatingEngine re2;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();

        MetadataSegment segment = constructSegment(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(),
                createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));

        re1 = new RatingEngine();
        re1.setSegment(retrievedSegment);
        re1.setCreatedBy(CREATED_BY);
        re1.setType(RatingEngineType.RULE_BASED);
        re1.setNote(RATING_ENGINE_NOTE_1);
        re2 = new RatingEngine();
        re2.setSegment(retrievedSegment);
        re2.setCreatedBy(CREATED_BY);
        re2.setType(RatingEngineType.AI_BASED);
    }

    @Test(groups = "deployment")
    public void testCreate() {
        testCreate(re1);
        testRatingEngineNoteCreation(re1, true);
        testCreate(re2);
        testRatingEngineNoteCreation(re2, false);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
    public void testGet() {
        // test get all rating engine summary list
        List<RatingEngineSummary> ratingEngineSummaries = ratingEngineProxy
                .getRatingEngineSummaries(mainTestTenant.getId());
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);
        log.info("ratingEngineSummaries is " + ratingEngineSummaries);
        String id1 = re1.getId();
        String id2 = re2.getId();
        RatingEngineSummary possibleRatingEngineSummary1 = null;
        RatingEngineSummary possibleRatingEngineSummary2 = null;
        for (RatingEngineSummary r : ratingEngineSummaries) {
            if (r.getId().equals(id1)) {
                possibleRatingEngineSummary1 = r;
            } else if (r.getId().equals(id2)) {
                possibleRatingEngineSummary2 = r;
            }
        }
        Assert.assertNotNull(possibleRatingEngineSummary1);
        Assert.assertNotNull(possibleRatingEngineSummary2);
        Assert.assertEquals(possibleRatingEngineSummary1.getSegmentDisplayName(),
                possibleRatingEngineSummary2.getSegmentDisplayName());
        Assert.assertEquals(possibleRatingEngineSummary1.getSegmentDisplayName(), SEGMENT_NAME);

        // test get all rating engine summary list filtered by type and status
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE", null);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE", null);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE",
                "RULE_BASED");
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE",
                "RULE_BASED");
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE",
                "AI_BASED");
        Assert.assertEquals(ratingEngineSummaries.size(), 1);

        // test RuleBased rating engine
        RatingEngine ruleRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), re1.getId());
        Assert.assertNotNull(ruleRatingEngine);
        Assert.assertEquals(ruleRatingEngine.getId(), re1.getId());
        MetadataSegment segment = ruleRatingEngine.getSegment();
        Assert.assertNotNull(segment);
        log.info("After loading, ratingEngine is " + ruleRatingEngine);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);

        Set<RatingModel> ratingModels = ruleRatingEngine.getRatingModels();
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Iterator<RatingModel> it = ratingModels.iterator();
        RatingModel rm = it.next();
        Assert.assertTrue(rm instanceof RuleBasedModel);
        Assert.assertEquals(rm.getIteration(), 1);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);

        // test AI rating engine
        RatingEngine aiRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), re2.getId());
        Assert.assertNotNull(aiRatingEngine);
        Assert.assertEquals(aiRatingEngine.getId(), re2.getId());
        segment = ruleRatingEngine.getSegment();
        Assert.assertNotNull(segment);
        log.info("After loading, ratingEngine is " + aiRatingEngine);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);

        ratingModels = aiRatingEngine.getRatingModels();
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        it = ratingModels.iterator();
        rm = it.next();
        Assert.assertTrue(rm instanceof AIModel);
        Assert.assertEquals(rm.getIteration(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGet" })
    public void testUpdate() {
        // test update rating engine
        testUpdateRuleBasedModel();
        testUpdateAIModel();
    }

    protected void testUpdateRuleBasedModel() {
        re1.setDisplayName(RATING_ENGINE_NAME_1);
        re1.setStatus(RatingEngineStatus.ACTIVE);
        re1.setNote(RATING_ENGINE_NEW_NOTE);
        RatingEngine ratingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), re1);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(RATING_ENGINE_NAME_1, ratingEngine.getDisplayName());
        Assert.assertEquals(re1.getId(), ratingEngine.getId());
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.ACTIVE);

        List<RatingEngineSummary> ratingEngineSummaries = ratingEngineProxy
                .getRatingEngineSummaries(mainTestTenant.getId());
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);

        ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), re1.getId());
        Assert.assertEquals(RATING_ENGINE_NAME_1, ratingEngine.getDisplayName());
        Assert.assertEquals(ratingEngine.getId(), re1.getId());
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.ACTIVE);

        // test update rating engine note
        List<RatingEngineNote> ratingEngineNotes = ratingEngineProxy.getAllNotes(mainTestTenant.getId(),
                ratingEngine.getId());
        Assert.assertNotNull(ratingEngineNotes);
        Assert.assertEquals(ratingEngineNotes.size(), 2);
        Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE_1);
        Assert.assertEquals(ratingEngineNotes.get(1).getNotesContents(), RATING_ENGINE_NEW_NOTE);

        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE", null);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE", null);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), null, "AI_BASED");
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), null, "RULE_BASED");
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);

        // test update rule based model
        Set<RatingModel> ratingModels = ratingEngineProxy.getRatingModels(mainTestTenant.getId(), re1.getId());
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Iterator<RatingModel> it = ratingModels.iterator();
        RatingModel rm = it.next();
        Assert.assertTrue(rm instanceof RuleBasedModel);
        Assert.assertEquals(rm.getIteration(), 1);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);

        String ratingModelId = rm.getId();
        Assert.assertNotNull(ratingModelId);
        rm = ratingEngineProxy.getRatingModel(mainTestTenant.getId(), re1.getId(), ratingModelId);
        Assert.assertNotNull(rm);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RuleBucketName.D.getName());
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setSelectedAttributes(generateSeletedAttributes());
        rm = ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), re1.getId(), ratingModelId, ruleBasedModel);
        Assert.assertNotNull(rm);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(), RuleBucketName.D.getName());
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR1));
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR2));
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR3));
    }

    protected void testUpdateAIModel() {
        re2.setDisplayName(RATING_ENGINE_NAME_2);
        re2.setStatus(RatingEngineStatus.ACTIVE);
        RatingEngine ratingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), re2);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(RATING_ENGINE_NAME_2, ratingEngine.getDisplayName());
        Assert.assertEquals(re2.getId(), ratingEngine.getId());
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.ACTIVE);

        List<RatingEngineSummary> ratingEngineSummaries = ratingEngineProxy
                .getRatingEngineSummaries(mainTestTenant.getId());
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);

        ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), re2.getId());
        Assert.assertEquals(RATING_ENGINE_NAME_2, ratingEngine.getDisplayName());
        Assert.assertEquals(ratingEngine.getId(), re2.getId());
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.ACTIVE);

        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE", null);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE", null);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), null, "AI_BASED");
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), null, "RULE_BASED");
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);

        // test update rule based model
        Set<RatingModel> ratingModels = ratingEngineProxy.getRatingModels(mainTestTenant.getId(), re2.getId());
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Iterator<RatingModel> it = ratingModels.iterator();
        RatingModel rm = it.next();
        Assert.assertTrue(rm instanceof AIModel);
        Assert.assertEquals(rm.getIteration(), 1);

        String ratingModelId = rm.getId();
        Assert.assertNotNull(ratingModelId);
        rm = ratingEngineProxy.getRatingModel(mainTestTenant.getId(), re2.getId(), ratingModelId);
        Assert.assertNotNull(rm);
        Assert.assertTrue(rm instanceof AIModel);

        ((AIModel) rm).setWorkflowType(ModelWorkflowType.CROSS_SELL);
        rm = ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), re2.getId(), ratingModelId, rm);
        Assert.assertNotNull(rm);

        rm = ratingEngineProxy.getRatingModel(mainTestTenant.getId(), re2.getId(), ratingModelId);
        Assert.assertNotNull(rm);
        Assert.assertTrue(rm instanceof AIModel);
        AIModel aiModel = (AIModel) rm;
        Assert.assertEquals(aiModel.getWorkflowType(), ModelWorkflowType.CROSS_SELL);
    }

    private void testCreate(RatingEngine re) {
        RatingEngine createdRe = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), re);
        Assert.assertNotNull(createdRe);
        re.setId(createdRe.getId());
        Assert.assertNotNull(createdRe.getRatingModels());
        Assert.assertNotNull(new ArrayList<>(createdRe.getRatingModels()));

        if (createdRe.getActiveModel() instanceof RuleBasedModel) {
            RuleBasedModel ruModel = (RuleBasedModel) createdRe.getActiveModel();
            Assert.assertNotNull(ruModel);
            Assert.assertNotNull(ruModel.getSelectedAttributes());
            Assert.assertTrue(ruModel.getSelectedAttributes().size() > 0);
        } else if (createdRe.getActiveModel() instanceof AIModel) {
            AIModel ruModel = (AIModel) createdRe.getActiveModel();
            Assert.assertNotNull(ruModel);
        }
    }

    private void testRatingEngineNoteCreation(RatingEngine ratingEngine, boolean shouldHaveRatingEngineNote) {
        if (shouldHaveRatingEngineNote) {
            List<RatingEngineNote> ratingEngineNotes = ratingEngineProxy.getAllNotes(mainTestTenant.getId(),
                    ratingEngine.getId());
            Assert.assertNotNull(ratingEngineNotes);
            Assert.assertEquals(ratingEngineNotes.size(), 1);
            Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE_1);
        } else {
            List<RatingEngineNote> ratingEngineNotes = ratingEngineProxy.getAllNotes(mainTestTenant.getId(),
                    ratingEngine.getId());
            Assert.assertTrue(CollectionUtils.isEmpty(ratingEngineNotes));
        }
    }

    private List<String> generateSeletedAttributes() {
        List<String> selectedAttributes = new ArrayList<>();
        selectedAttributes.add(ATTR1);
        selectedAttributes.add(ATTR2);
        selectedAttributes.add(ATTR3);
        return selectedAttributes;
    }

    @Test(groups = "deployment", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        ratingEngineProxy.deleteRatingEngine(mainTestTenant.getId(), re1.getId());
        ratingEngineProxy.deleteRatingEngine(mainTestTenant.getId(), re2.getId());
        List<RatingEngineSummary> ratingEngineSummaries = ratingEngineProxy
                .getRatingEngineSummaries(mainTestTenant.getId());
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
    }

}
