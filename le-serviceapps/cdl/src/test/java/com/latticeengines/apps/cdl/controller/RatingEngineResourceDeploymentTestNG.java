package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineAndActionDTO;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelAndActionDTO;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class RatingEngineResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResourceDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME_1 = "Rating Engine 1";
    private static final String RATING_ENGINE_NOTE_1 = "This is a Rating Engine that covers North America market";
    private static final String RATING_ENGINE_NEW_NOTE = "This is a Rating Engine that covers East Asia market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final String ATTR1 = "Employ Number";
    private static final String ATTR2 = "Revenue";
    private static final String ATTR3 = "Has Cisco WebEx";

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private RatingEngine re1;
    private RatingEngine re2;
    private final boolean shouldCreateActionWithRatingEngine1 = true;
    private final boolean shouldCreateActionWithRatingEngine2 = false;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
        MetadataSegment segment = constructSegment(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(),
                createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));

        re1 = createRuleBasedRatingEngine(retrievedSegment);
        re2 = createAIRatingEngine(retrievedSegment);
    }

    @Test(groups = "deployment")
    public void testCreate() {
        testCreate(re1);
        // Only mock the Rulebased Rating data in Redshift to test the filtering
        // logic
        cdlTestDataService.mockRatingTableWithSingleEngine(mainTestTenant.getId(), re1.getId(), null);
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
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE", null);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE",
                "RULE_BASED");
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE",
                "RULE_BASED");
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE",
                "CROSS_SELL");
        Assert.assertEquals(ratingEngineSummaries.size(), 1);

        // test Rating Attribute in Redshift
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE",
                "CROSS_SELL", true);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE",
                "CROSS_SELL", false);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE",
                "RULE_BASED", true);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE",
                "RULE_BASED", false);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);

        // test RuleBased rating engine
        RatingEngine ruleRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), re1.getId());
        Assert.assertNotNull(ruleRatingEngine);
        Assert.assertEquals(ruleRatingEngine.getId(), re1.getId());
        MetadataSegment segment = ruleRatingEngine.getSegment();
        Assert.assertNotNull(segment);
        log.info("After loading, ratingEngine is " + ruleRatingEngine);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);

        RatingModel ratingModel = ruleRatingEngine.getActiveModel();
        Assert.assertNotNull(ratingModel);
        Assert.assertTrue(ratingModel instanceof RuleBasedModel);
        Assert.assertEquals(((RuleBasedModel) ratingModel).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);

        // test AI rating engine
        RatingEngine aiRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), re2.getId());
        Assert.assertNotNull(aiRatingEngine);
        Assert.assertEquals(aiRatingEngine.getId(), re2.getId());
        segment = ruleRatingEngine.getSegment();
        Assert.assertNotNull(segment);
        log.info("After loading, ratingEngine is " + aiRatingEngine);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);

        List<RatingModel> ratingModels = ratingEngineProxy.getRatingModels(mainTestTenant.getId(), re2.getId());
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        ratingModel = ratingModels.get(0);
        Assert.assertTrue(ratingModel instanceof AIModel);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGet" })
    public void testUpdate() {
        // test update rating engine
        testUpdateRuleBasedModel();
    }

    protected void testUpdateRuleBasedModel() {
        re1.setDisplayName(RATING_ENGINE_NAME_1);
        re1.setStatus(RatingEngineStatus.INACTIVE);
        re1.setNote(RATING_ENGINE_NEW_NOTE);
        RatingEngine ratingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), re1);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(RATING_ENGINE_NAME_1, ratingEngine.getDisplayName());
        Assert.assertEquals(re1.getId(), ratingEngine.getId());
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.INACTIVE);

        List<RatingEngineSummary> ratingEngineSummaries = ratingEngineProxy
                .getRatingEngineSummaries(mainTestTenant.getId());
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);

        // test Rating Attribute in Redshift
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), null, null, true);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        Assert.assertEquals(ratingEngineSummaries.get(0).getId(), re1.getId());

        ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), re1.getId());
        Assert.assertEquals(RATING_ENGINE_NAME_1, ratingEngine.getDisplayName());
        Assert.assertEquals(ratingEngine.getId(), re1.getId());
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.INACTIVE);

        // test update rating engine note
        List<RatingEngineNote> ratingEngineNotes = ratingEngineProxy.getAllNotes(mainTestTenant.getId(),
                ratingEngine.getId());
        Assert.assertNotNull(ratingEngineNotes);
        Assert.assertEquals(ratingEngineNotes.size(), 2);
        Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE_1);
        Assert.assertEquals(ratingEngineNotes.get(1).getNotesContents(), RATING_ENGINE_NEW_NOTE);

        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "ACTIVE", null);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), "INACTIVE", null);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), null, "CROSS_SELL");
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId(), null, "RULE_BASED");
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);

        // test update rule based model
        List<RatingModel> ratingModels = ratingEngineProxy.getRatingModels(mainTestTenant.getId(), re1.getId());
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Iterator<RatingModel> it = ratingModels.iterator();
        RatingModel rm = it.next();
        Assert.assertTrue(rm instanceof RuleBasedModel);
        Assert.assertEquals(rm.getIteration(), 1);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);

        String ratingModelId = rm.getId();
        log.info("ratingModelId is " + ratingModelId);
        Assert.assertNotNull(ratingModelId);
        rm = ratingEngineProxy.getRatingModel(mainTestTenant.getId(), re1.getId(), ratingModelId);
        Assert.assertNotNull(rm);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RatingBucketName.D.getName());
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setSelectedAttributes(generateSeletedAttributes());
        RatingModelAndActionDTO rmAndActionDTO = ratingEngineProxy.updateRatingModelAndActionDTO(mainTestTenant.getId(),
                re1.getId(), ratingModelId, ruleBasedModel);
        log.info("rmAndActionDTO is " + rmAndActionDTO);
        rm = rmAndActionDTO.getRatingModel();
        Assert.assertNotNull(rm);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(), RatingBucketName.D.getName());
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR1));
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR2));
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR3));
        Action action = rmAndActionDTO.getAction();
        assertRuleBasedModelUpdateAction(action, ratingEngine, ratingModelId);

        // update only the selected attributes
        ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setSelectedAttributes(generateSeletedAttributes());
        rmAndActionDTO = ratingEngineProxy.updateRatingModelAndActionDTO(mainTestTenant.getId(), re1.getId(),
                ratingModelId, ruleBasedModel);
        log.info("Second time rmAndActionDTO is " + rmAndActionDTO);
        rm = rmAndActionDTO.getRatingModel();
        Assert.assertNotNull(rm);
        action = rmAndActionDTO.getAction();
        Assert.assertNull(action);
    }

    private RatingEngine createRuleBasedRatingEngine(MetadataSegment retrievedSegment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE_1);
        if (shouldCreateActionWithRatingEngine1) {
            ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        }
        return ratingEngine;
    }

    private RatingEngine createAIRatingEngine(MetadataSegment retrievedSegment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.CROSS_SELL);
        if (shouldCreateActionWithRatingEngine2) {
            ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        }
        return ratingEngine;
    }

    private void testCreate(RatingEngine re) {
        RatingEngineAndActionDTO createdReAndActionDTO = ratingEngineProxy
                .createOrUpdateRatingEngineAndActionDTO(mainTestTenant.getId(), re);
        Assert.assertNotNull(createdReAndActionDTO);
        RatingEngine createdRe = createdReAndActionDTO.getRatingEngine();
        Assert.assertNotNull(createdRe);
        Action action = createdReAndActionDTO.getAction();
        re.setId(createdRe.getId());
        Assert.assertNotNull(createdRe.getActiveModel());
        RatingEngine retrievedRe = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), createdRe.getId());

        if (retrievedRe.getActiveModel() instanceof RuleBasedModel) {
            Assert.assertNotNull(retrievedRe.getActiveModel());
            RuleBasedModel ruModel = (RuleBasedModel) retrievedRe.getActiveModel();
            Assert.assertNotNull(ruModel);
            Assert.assertNotNull(ruModel.getSelectedAttributes());
            Assert.assertTrue(ruModel.getSelectedAttributes().size() > 0);
            if (shouldCreateActionWithRatingEngine1) {
                assertRatingEngineActivationAction(action, createdRe);
            } else {
                Assert.assertNull(action);
            }
        } else if (retrievedRe.getActiveModel() instanceof AIModel) {
            AIModel aiModel = (AIModel) retrievedRe.getActiveModel();
            Assert.assertNotNull(aiModel);
            if (shouldCreateActionWithRatingEngine2) {
                // do nothing for now
            } else {
                Assert.assertNull(action);
            }
        }
    }

    private void assertRatingEngineActivationAction(Action action, RatingEngine ratingEngine) {
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);
        Assert.assertEquals(action.getActionInitiator(), ratingEngine.getCreatedBy());
        Assert.assertTrue(action.getActionConfiguration() instanceof RatingEngineActionConfiguration);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getRatingEngineId(),
                ratingEngine.getId());
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getSubType(),
                RatingEngineActionConfiguration.SubType.ACTIVATION);
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.ACTIVE);
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

    private void assertRuleBasedModelUpdateAction(Action action, RatingEngine ratingEngine, String ratingModelId) {
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);
        Assert.assertEquals(action.getActionInitiator(), ratingEngine.getCreatedBy());
        Assert.assertTrue(action.getActionConfiguration() instanceof RatingEngineActionConfiguration);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getRatingEngineId(),
                ratingEngine.getId());
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getModelId(),
                ratingModelId);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getSubType(),
                RatingEngineActionConfiguration.SubType.RULE_MODEL_BUCKET_CHANGE);
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
