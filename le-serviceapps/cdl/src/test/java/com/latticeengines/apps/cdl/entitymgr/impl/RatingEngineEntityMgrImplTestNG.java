package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineNoteEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class RatingEngineEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineEntityMgrImplTestNG.class);

    private static final String PLAY_NAME = "PLAY HARD";
    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String RATING_ENGINE_NEW_NOTE = "This is a Rating Engine that covers East Asia market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private RatingEngineNoteEntityMgr ratingEngineNoteEntityMgr;

    @Inject
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Inject
    private AIModelEntityMgr aiModelEntityMgr;

    private RatingEngine ratingEngine;

    private String ratingEngineId;

    private RatingEngine createdRatingEngine;

    private RatingEngine crossSellRatingEngine;

    private RatingEngine customEventRatingEngine;

    private String crossSellRatingEngineId;

    private String customEventRatingEngineId;

    private List<RatingEngineNote> ratingEngineNotes;

    private List<RatingEngine> ratingEngineList;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
        ratingEngine = new RatingEngine();
        ratingEngine.setSegment(testSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        ratingEngine.setId(UUID.randomUUID().toString());

        crossSellRatingEngine = new RatingEngine();
        crossSellRatingEngine.setSegment(testSegment);
        crossSellRatingEngine.setCreatedBy(CREATED_BY);
        crossSellRatingEngine.setType(RatingEngineType.CROSS_SELL);
        crossSellRatingEngine.setNote(RATING_ENGINE_NOTE);
        crossSellRatingEngine.setId(UUID.randomUUID().toString());

        customEventRatingEngine = new RatingEngine();
        customEventRatingEngine.setSegment(testSegment);
        customEventRatingEngine.setCreatedBy(CREATED_BY);
        customEventRatingEngine.setType(RatingEngineType.CUSTOM_EVENT);
        customEventRatingEngine.setNote(RATING_ENGINE_NOTE);
        customEventRatingEngine.setId(UUID.randomUUID().toString());

        ActionContext.remove();
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        ActionContext.remove();
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "functional")
    public void testCreation() {
        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngine);

        log.info("Rating Engine is " + createdRatingEngine.toString());
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        validateRatingModelCreation(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        ratingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        Assert.assertEquals(ratingEngineId, createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine.getCreated());
        Assert.assertNotNull(createdRatingEngine.getUpdated());
        Assert.assertNotNull(createdRatingEngine.getDisplayName());
        Assert.assertNull(createdRatingEngine.getNote());
        Assert.assertEquals(createdRatingEngine.getType(), RatingEngineType.RULE_BASED);
        Assert.assertEquals(createdRatingEngine.getCreatedBy(), CREATED_BY);
        Assert.assertEquals(createdRatingEngine.getStatus(), RatingEngineStatus.INACTIVE);
        Assert.assertTrue(MapUtils.isEmpty(createdRatingEngine.getCountsAsMap()));

        // test rating engine note creation
        ratingEngineNotes = ratingEngineNoteEntityMgr.getAllByRatingEngine(createdRatingEngine);
        Assert.assertNotNull(ratingEngineNotes);
        Assert.assertEquals(ratingEngineNotes.size(), 1);
        Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE);
        log.info(String.format("Rating Engine Note is %s", ratingEngineNotes.get(0)));

        String createdRatingEngineStr = createdRatingEngine.toString();
        log.info("createdRatingEngineStr is " + createdRatingEngineStr);
        createdRatingEngine = JsonUtils.deserialize(createdRatingEngineStr, RatingEngine.class);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        MetadataSegment segment = createdRatingEngine.getSegment();
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);
        DataCollection dc = segment.getDataCollection();
        log.info("dc is " + dc);
        log.info("Rating Engine after findById is " + createdRatingEngine.toString());

        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(crossSellRatingEngine);

        log.info("Rating Engine is " + createdRatingEngine.toString());
        Assert.assertNotNull(createdRatingEngine);
        validateAIRatingModelCreation(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        crossSellRatingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(crossSellRatingEngineId);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        Assert.assertNull(createdRatingEngine.getScoringIteration());
        Assert.assertNull(createdRatingEngine.getPublishedIteration());
        Assert.assertEquals(crossSellRatingEngineId, createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine.getCreated());
        Assert.assertNotNull(createdRatingEngine.getUpdated());
        Assert.assertNotNull(createdRatingEngine.getDisplayName());
        Assert.assertNull(createdRatingEngine.getNote());
        Assert.assertEquals(createdRatingEngine.getType(), RatingEngineType.CROSS_SELL);
        Assert.assertEquals(createdRatingEngine.getCreatedBy(), CREATED_BY);
        Assert.assertEquals(createdRatingEngine.getStatus(), RatingEngineStatus.INACTIVE);

        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(customEventRatingEngine);
        customEventRatingEngineId = createdRatingEngine.getId();
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        Assert.assertNull(createdRatingEngine.getScoringIteration());
        Assert.assertNull(createdRatingEngine.getPublishedIteration());
        Assert.assertEquals(createdRatingEngine.getType(), RatingEngineType.CUSTOM_EVENT);
        Assert.assertEquals(createdRatingEngine.getCreatedBy(), CREATED_BY);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreation" })
    public void testFind() {
        // test find all
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 3);

        // test find ids
        List<String> ratingEngineIds = ratingEngineEntityMgr.findAllIdsInSegment(testSegment.getName());
        Assert.assertNotNull(ratingEngineIds);
        Assert.assertEquals(ratingEngineIds.size(), 3);

        ratingEngineIds = ratingEngineEntityMgr.findAllIdsInSegment("NoSuchSegment");
        Assert.assertNotNull(ratingEngineIds);
        Assert.assertEquals(ratingEngineIds.size(), 0);

        // test find all by type and status
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, null);
        Assert.assertEquals(ratingEngineList.size(), 3);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.CROSS_SELL.name(), null);
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(), null);
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 3);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);

        // test find with active model populated
        RatingEngine re = ratingEngineEntityMgr.findById(ratingEngineId, true);
        Assert.assertNotNull(re.getLatestIteration());
        RuleBasedModel latestIteration = (RuleBasedModel) re.getLatestIteration();
        validateDefaultRuleBasedModel(latestIteration);
        validateSelectedAttributesInRuleBasedModel(latestIteration);

    }

    @SuppressWarnings("deprecation")
    @Test(groups = "functional", dependsOnMethods = { "testFind" })
    public void testUpdate() {
        // test update
        RatingEngine re = new RatingEngine();
        re.setDisplayName(RATING_ENGINE_NAME);
        re.setNote(RATING_ENGINE_NEW_NOTE);
        re.setId(ratingEngine.getId());
        re.setStatus(RatingEngineStatus.ACTIVE);
        re.setScoringIteration(ratingEngine.getLatestIteration());
        re.setCountsByMap(ImmutableMap.of( //
                RatingBucketName.A.getName(), 1L, //
                RatingBucketName.B.getName(), 2L, //
                RatingBucketName.C.getName(), 3L));
        createdRatingEngine = ratingEngineEntityMgr.updateRatingEngine(re,
                ratingEngineEntityMgr.findById(ratingEngine.getId()), false);

        log.info("Rating Engine after update is " + createdRatingEngine.toString());
        Assert.assertEquals(createdRatingEngine.getStatus(), RatingEngineStatus.ACTIVE);
        Assert.assertNotNull(createdRatingEngine.getScoringIteration());
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());

        validateRatingModelCreation(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        Assert.assertEquals(RATING_ENGINE_NAME, createdRatingEngine.getDisplayName());
        validateActionContext(createdRatingEngine.getId(), RatingEngineActionConfiguration.SubType.ACTIVATION);

        log.info("The update date for the newly updated one is "
                + ratingEngineEntityMgr.findById(ratingEngine.getId()).getUpdated());
        log.info("The create date for the newly updated one is " + createdRatingEngine.getCreated());

        re = new RatingEngine();
        re.setId(customEventRatingEngine.getId());
        re.setSegment(null);
        createdRatingEngine = ratingEngineEntityMgr.updateRatingEngine(re,
                ratingEngineEntityMgr.findById(customEventRatingEngine.getId()), true);

        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        AIModel latestIteration = (AIModel) createdRatingEngine.getLatestIteration();
        Assert.assertNotNull(latestIteration);
        CustomEventModelingConfig config = (CustomEventModelingConfig) latestIteration.getAdvancedModelingConfig();
        Assert.assertEquals(config.getCustomEventModelingType(), CustomEventModelingType.LPI);

        // Ai Ratings set scoring Iteration first, then mark them as active
        re = new RatingEngine();
        re.setDisplayName(RATING_ENGINE_NAME);
        re.setNote(RATING_ENGINE_NEW_NOTE);
        re.setId(crossSellRatingEngine.getId());
        re.setScoringIteration(crossSellRatingEngine.getLatestIteration());
        createdRatingEngine = ratingEngineEntityMgr.updateRatingEngine(re,
                ratingEngineEntityMgr.findById(crossSellRatingEngine.getId()), false);

        re = new RatingEngine();
        re.setId(crossSellRatingEngine.getId());
        re.setStatus(RatingEngineStatus.ACTIVE);
        createdRatingEngine = ratingEngineEntityMgr.updateRatingEngine(re,
                ratingEngineEntityMgr.findById(crossSellRatingEngine.getId()), false);

        log.info("Rating Engine after update is " + createdRatingEngine.toString());
        Assert.assertEquals(createdRatingEngine.getStatus(), RatingEngineStatus.ACTIVE);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        Assert.assertNotNull(createdRatingEngine.getScoringIteration());
        Assert.assertNull(createdRatingEngine.getPublishedIteration());
        validateAIRatingModelCreation(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        Assert.assertEquals(RATING_ENGINE_NAME, createdRatingEngine.getDisplayName());
        validateActionContext(createdRatingEngine.getId(), RatingEngineActionConfiguration.SubType.ACTIVATION);

        // test rating engine note update
        ratingEngineNotes = ratingEngineNoteEntityMgr.getAllByRatingEngine(createdRatingEngine);
        Assert.assertNotNull(ratingEngineNotes);
        Assert.assertEquals(ratingEngineNotes.size(), 2);
        Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE);
        Assert.assertEquals(ratingEngineNotes.get(1).getNotesContents(), RATING_ENGINE_NEW_NOTE);
        log.info(String.format("Rating Engine Note is %s", ratingEngineNotes.get(0)));

        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 3);

        RatingEngine retrievedRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        log.info("Rating Engine after update is " + retrievedRatingEngine.toString());
        Map<String, Long> counts = retrievedRatingEngine.getCountsAsMap();
        Assert.assertTrue(MapUtils.isNotEmpty(counts));
        Assert.assertEquals(counts.get(RatingBucketName.A.getName()), new Long(1));
        Assert.assertEquals(counts.get(RatingBucketName.B.getName()), new Long(2));
        Assert.assertEquals(counts.get(RatingBucketName.C.getName()), new Long(3));

        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 2);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);

        // Soft Delete should fail since there are Plays associated with Rating
        // Engine
        Play play = generateDefaultPlay(createdRatingEngine);
        playEntityMgr.createPlay(play);
        re = ratingEngineEntityMgr.findById(createdRatingEngine.getId());
        try {
            ratingEngineEntityMgr.deleteById(re.getId(), false, CREATED_BY);
            Assert.fail("Should have thrown exeption due to the transition should fail");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            // TODO -enable it
            // Assert.assertTrue(e instanceof LedpException);
            // Assert.assertEquals(((LedpException) e).getCode(),
            // LedpCode.LEDP_18175);
        }

        // Soft Delete should fail since Rating is still active
        play.setDeleted(true);
        playEntityMgr.updatePlay(play, playEntityMgr.getPlayByName(play.getName(), false));
        try {
            ratingEngineEntityMgr.deleteById(re.getId(), false, CREATED_BY);
            Assert.fail("Should have thrown exeption due to the transition should fail");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            // TODO - enable it
            // Assert.assertTrue(e instanceof LedpException);
            // Assert.assertEquals(((LedpException) e).getCode(),
            // LedpCode.LEDP_18181);
        }

        re = new RatingEngine();
        re.setId(ratingEngine.getId());
        re.setStatus(RatingEngineStatus.INACTIVE);
        ratingEngineEntityMgr.updateRatingEngine(re, ratingEngineEntityMgr.findById(ratingEngine.getId()), false);

        re = ratingEngineEntityMgr.findById(ratingEngine.getId());
        // Soft Delete should now succeed
        ratingEngineEntityMgr.deleteById(re.getId(), false, CREATED_BY);
        RatingEngine retrievedRe = ratingEngineEntityMgr.findById(ratingEngine.getId());
        Assert.assertNotNull(retrievedRe);
        Assert.assertTrue(retrievedRe.getDeleted());
        ratingEngineList = ratingEngineEntityMgr.findAllDeleted();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.get(0).getId(), ratingEngine.getId());
        Assert.assertTrue(ratingEngineList.get(0).getDeleted());

        // Revert soft delete Rating Engine
        ratingEngineEntityMgr.revertDelete(ratingEngine.getId());
        retrievedRe = ratingEngineEntityMgr.findById(ratingEngine.getId());
        Assert.assertNotNull(retrievedRe);
        Assert.assertFalse(retrievedRe.getDeleted());
        ratingEngineList = ratingEngineEntityMgr.findAllDeleted();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);

        // Soft Delete again
        ratingEngineEntityMgr.deleteById(ratingEngine.getId(), false, CREATED_BY);
        retrievedRe = ratingEngineEntityMgr.findById(ratingEngine.getId());
        Assert.assertNotNull(retrievedRe);
        Assert.assertTrue(retrievedRe.getDeleted());

        re = new RatingEngine();
        re.setId(crossSellRatingEngineId);
        re.setStatus(RatingEngineStatus.INACTIVE);
        ratingEngineEntityMgr.updateRatingEngine(re, ratingEngineEntityMgr.findById(crossSellRatingEngineId), false);

        ratingEngineEntityMgr.deleteById(crossSellRatingEngineId, false, CREATED_BY);
        retrievedRe = ratingEngineEntityMgr.findById(crossSellRatingEngineId);
        Assert.assertNotNull(retrievedRe);
        Assert.assertTrue(retrievedRe.getDeleted());

        ratingEngineList = ratingEngineEntityMgr.findAllDeleted();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 2);
        Assert.assertTrue(ratingEngineList.get(0).getDeleted());
        Assert.assertTrue(ratingEngineList.get(1).getDeleted());
    }

    private void validateActionContext(String ratingEngineId, RatingEngineActionConfiguration.SubType subType) {
        Action action = ActionContext.getAction();
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);
        Assert.assertEquals(action.getActionInitiator(), CREATED_BY);
        Assert.assertTrue(action.getActionConfiguration() instanceof RatingEngineActionConfiguration);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getSubType(), subType);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getRatingEngineId(),
                ratingEngineId);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDeletion() {
        ratingEngineEntityMgr.deleteById(ratingEngineId, true, CREATED_BY);
        ratingEngineEntityMgr.deleteById(crossSellRatingEngineId, true, CREATED_BY);
        ratingEngineEntityMgr.deleteById(customEventRatingEngineId, true, CREATED_BY);

        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);

        createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNull(createdRatingEngine);
        createdRatingEngine = ratingEngineEntityMgr.findById(crossSellRatingEngineId);
        Assert.assertNull(createdRatingEngine);
        createdRatingEngine = ratingEngineEntityMgr.findById(customEventRatingEngineId);
        Assert.assertNull(createdRatingEngine);
    }

    private void validateRatingModelCreation(RatingEngine ratingEngine) {
        List<RuleBasedModel> ratingModels = ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngine.getId());
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        validateDefaultRuleBasedModel(ratingModels.get(0));
    }

    private void validateAIRatingModelCreation(RatingEngine ratingEngine) {
        List<AIModel> ratingModels = aiModelEntityMgr.findAllByRatingEngineId(ratingEngine.getId(), null);
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Assert.assertEquals(ratingModels.get(0).getModelingJobStatus(), JobStatus.PENDING);
        Assert.assertEquals(ratingModels.get(0).getAdvancedModelingConfig().getClass(), CrossSellModelingConfig.class);
    }

    private void validateDefaultRuleBasedModel(RuleBasedModel model) {
        Assert.assertEquals(model.getRatingRule().getDefaultBucketName(), RatingRule.DEFAULT_BUCKET_NAME);
    }

    private void validateSelectedAttributesInRuleBasedModel(RuleBasedModel model) {
        Assert.assertNotNull(model.getSelectedAttributes());
        Assert.assertTrue(model.getSelectedAttributes().size() > 0);
    }

    @Test(groups = "functional")
    public void testFindUsedAttributes() {
        RatingEngineEntityMgrImpl r = new RatingEngineEntityMgrImpl();

        MetadataSegment segment = new MetadataSegment();
        segment.setAccountRestriction(testSegment.getAccountRestriction());
        segment.setContactRestriction(testSegment.getContactRestriction());
        List<String> usedAttributesInSegment = r.findUsedAttributes(segment);
        Assert.assertNotNull(usedAttributesInSegment);

        Set<String> expectedResult = new HashSet<>();
        addAttrInExpectedSet(expectedResult, BusinessEntity.Account, accountAttributes);
        addAttrInExpectedSet(expectedResult, BusinessEntity.Contact, contactAttributes);

        Assert.assertEquals(usedAttributesInSegment.size(), (accountAttributes.size() + contactAttributes.size()));
        Assert.assertEquals(usedAttributesInSegment.size(), expectedResult.size());

        for (String attr : usedAttributesInSegment) {
            if (!expectedResult.contains(attr)) {
                log.info("Selected attribute not expected: " + attr);
            }
            Assert.assertTrue(expectedResult.contains(attr));
        }
    }

    private void addAttrInExpectedSet(Set<String> expectedResult, BusinessEntity entity,
            List<String> entityAttributes) {
        for (String attr : entityAttributes) {
            expectedResult.add(entity + "." + attr);
        }
    }

    private Play generateDefaultPlay(RatingEngine ratingEngine) {
        List<PlayType> types = playTypeService.getAllPlayTypes(mainTestTenant.getId());
        Play play = new Play();
        play.setDisplayName(PLAY_NAME);
        play.setDescription(PLAY_NAME);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setRatingEngine(ratingEngine);
        play.setPlayStatus(PlayStatus.INACTIVE);
        play.setTenant(mainTestTenant);
        play.setPlayType(types.get(0));
        play.setName(UUID.randomUUID().toString());
        play.setTargetSegment(testSegment);
        return play;
    }
}
