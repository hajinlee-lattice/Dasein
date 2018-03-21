package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineNoteEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class RatingEngineEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineEntityMgrImplTestNG.class);

    private static final String PLAY_NAME = "PLAY HARD";
    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String RATING_ENGINE_NEW_NOTE = "This is a Rating Engine that covers East Asia market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String LDC_NAME = "LDC_Name";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private RatingEngineNoteEntityMgr ratingEngineNoteEntityMgr;

    @Inject
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    private RatingEngine ratingEngine;

    private RatingEngine createdRatingEngine;

    private String ratingEngineId;

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
        ActionContext.remove();
    }

    @AfterClass(groups = "functional")
    public void cleanupActionContext() {
        ActionContext.remove();
    }

    @Test(groups = "functional")
    public void testCreation() {
        createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine, mainTestTenant.getId());
        log.info("Rating Engine is " + createdRatingEngine.toString());
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getActiveModelPid());
        Assert.assertNotNull(createdRatingEngine.getActiveModel());
        validateRatingModelCreation(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        ratingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNull(createdRatingEngine.getActiveModel());
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
        MetadataSegment segment = createdRatingEngine.getSegment();
        Assert.assertNotNull(segment);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);
        DataCollection dc = segment.getDataCollection();
        log.info("dc is " + dc);
        log.info("Rating Engine after findById is " + createdRatingEngine.toString());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreation" })
    public void testFind() {
        // test find all
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(ratingEngineId, ratingEngineList.get(0).getId());

        // test find ids
        List<String> ratingEngineIds = ratingEngineEntityMgr.findAllIdsInSegment(testSegment.getName());
        Assert.assertNotNull(ratingEngineIds);
        Assert.assertEquals(ratingEngineIds.size(), 1);
        Assert.assertEquals(ratingEngineIds.get(0), ratingEngineId);

        ratingEngineIds = ratingEngineEntityMgr.findAllIdsInSegment("NoSuchSegment");
        Assert.assertNotNull(ratingEngineIds);
        Assert.assertEquals(ratingEngineIds.size(), 0);

        // test find all by type and status
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, null);
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.CROSS_SELL.name(), null);
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(), null);
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
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
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);

        // test find with active model populated
        RatingEngine re = ratingEngineEntityMgr.findById(ratingEngineId, true);
        Assert.assertNotNull(re.getActiveModel());
        RuleBasedModel activeModel = (RuleBasedModel) re.getActiveModel();
        validateDefaultRuleBasedModel(activeModel);
        validateSelectedAttributesInRuleBasedModel(activeModel);

    }

    @Test(groups = "functional", dependsOnMethods = { "testFind" })
    public void testUpdate() {
        // test update
        RatingEngine re = new RatingEngine();
        re.setDisplayName(RATING_ENGINE_NAME);
        re.setNote(RATING_ENGINE_NEW_NOTE);
        re.setStatus(RatingEngineStatus.ACTIVE);
        re.setId(ratingEngine.getId());
        re.setCountsByMap(ImmutableMap.of( //
                RatingBucketName.A.getName(), 1L, //
                RatingBucketName.B.getName(), 2L, //
                RatingBucketName.C.getName(), 3L));
        createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(re, mainTestTenant.getId());
        log.info("Rating Engine after update is " + createdRatingEngine.toString());
        Assert.assertEquals(createdRatingEngine.getStatus(), RatingEngineStatus.ACTIVE);
        Assert.assertNotNull(createdRatingEngine.getActiveModelPid());
        Assert.assertNull(createdRatingEngine.getActiveModel());
        validateRatingModelCreation(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        Assert.assertEquals(RATING_ENGINE_NAME, createdRatingEngine.getDisplayName());
        validateActionContext(createdRatingEngine);

        log.info("The update date for the newly updated one is "
                + ratingEngineEntityMgr.findById(ratingEngine.getId()).getUpdated());
        log.info("The create date for the newly updated one is " + createdRatingEngine.getCreated());

        // test rating engine note update
        ratingEngineNotes = ratingEngineNoteEntityMgr.getAllByRatingEngine(createdRatingEngine);
        Assert.assertNotNull(ratingEngineNotes);
        Assert.assertEquals(ratingEngineNotes.size(), 2);
        Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE);
        Assert.assertEquals(ratingEngineNotes.get(1).getNotesContents(), RATING_ENGINE_NEW_NOTE);
        log.info(String.format("Rating Engine Note is %s", ratingEngineNotes.get(0)));

        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(ratingEngineId, ratingEngineList.get(0).getId());
        RatingEngine retrievedRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        log.info("Rating Engine after update is " + retrievedRatingEngine.toString());
        Map<String, Long> counts = re.getCountsAsMap();
        Assert.assertTrue(MapUtils.isNotEmpty(counts));
        Assert.assertEquals(counts.get(RatingBucketName.A.getName()), new Long(1));
        Assert.assertEquals(counts.get(RatingBucketName.B.getName()), new Long(2));
        Assert.assertEquals(counts.get(RatingBucketName.C.getName()), new Long(3));

        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);

        // test soft delete and status transition
        Play play = generateDefaultPlay(createdRatingEngine);
        playEntityMgr.createOrUpdatePlay(play);
        re = new RatingEngine();
        re.setId(ratingEngine.getId());
        re.setStatus(RatingEngineStatus.INACTIVE);
        ratingEngineEntityMgr.createOrUpdateRatingEngine(re, mainTestTenant.getId());

        try {
            ratingEngineEntityMgr.deleteRatingEngine(re, false);
            Assert.fail("Should have thrown exeption due to the transition should fail");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LedpException);
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18175);
        }

        play.setPlayStatus(PlayStatus.DELETED);
        playEntityMgr.createOrUpdatePlay(play);
        ratingEngineEntityMgr.deleteRatingEngine(re, false);
        re = ratingEngineEntityMgr.findById(re.getId());
        Assert.assertTrue(re.getDeleted());
    }

    private void validateActionContext(RatingEngine ratingEngine) {
        Action action = ActionContext.getAction();
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);
        Assert.assertEquals(action.getActionInitiator(), CREATED_BY);
        Assert.assertTrue(action.getActionConfiguration() instanceof RatingEngineActionConfiguration);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getSubType(),
                RatingEngineActionConfiguration.SubType.ACTIVATION);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getRatingEngineId(),
                ratingEngine.getId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDeletion() {
        ratingEngineEntityMgr.deleteById(ratingEngineId);
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);

        createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNull(createdRatingEngine);
    }

    private void validateRatingModelCreation(RatingEngine ratingEngine) {
        List<RuleBasedModel> ratingModels = ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngine.getId());
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        validateDefaultRuleBasedModel(ratingModels.get(0));
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
        Play play = new Play();
        play.setDescription(PLAY_NAME);
        play.setCreatedBy(CREATED_BY);
        play.setRatingEngine(ratingEngine);
        play.setPlayStatus(PlayStatus.INACTIVE);
        play.setTenant(mainTestTenant);
        return play;
    }
}
