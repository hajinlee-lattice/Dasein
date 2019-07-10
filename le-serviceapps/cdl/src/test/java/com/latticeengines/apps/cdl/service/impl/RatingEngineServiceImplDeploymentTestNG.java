package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RatingEngineNoteService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class RatingEngineServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineServiceImplDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String RATING_ENGINE_NEW_NOTE = "This is a Rating Engine that covers East Asia market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private RatingEngineNoteService ratingEngineNoteService;

    @Inject
    private CDLTestDataService cdlTestDataService;

    private MetadataSegment reTestSegment;

    private RatingEngine rbRatingEngine;
    private String rbRatingEngineId;

    private RatingEngine aiRatingEngine;
    private String aiRatingEngineId;

    private Date createdDate;
    private Date updatedDate;

    @BeforeClass(groups = "deployment-app")
    public void setup() {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(),
                constructSegment(SEGMENT_NAME));
        Assert.assertNotNull(createdSegment);
        reTestSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", reTestSegment.getName()));
    }

    @Test(groups = "deployment-app")
    public void testCreate() {
        // Test Rulebased Rating Engine
        rbRatingEngine = createRatingEngine(RatingEngineType.RULE_BASED);
        Assert.assertEquals(rbRatingEngine.getType(), RatingEngineType.RULE_BASED);
        assertRatingEngine(rbRatingEngine);
        rbRatingEngineId = rbRatingEngine.getId();
        // Only mock the Rulebased Rating data in Redshift to test the filtering
        // logic
        cdlTestDataService.mockRatingTableWithSingleEngine(mainTestTenant.getId(), rbRatingEngineId, null);

        // Test AI Rating Engine
        aiRatingEngine = createRatingEngine(RatingEngineType.CROSS_SELL);
        Assert.assertEquals(aiRatingEngine.getType(), RatingEngineType.CROSS_SELL);
        assertRatingEngine(aiRatingEngine);
        aiRatingEngineId = aiRatingEngine.getId();
    }

    protected RatingEngine createRatingEngine(RatingEngineType type) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(reTestSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(type);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        // test basic creation
        ratingEngine = createOrUpdate(ratingEngine);

        return ratingEngine;
    }

    protected void assertRatingEngine(RatingEngine createdRatingEngine) {
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine.getCreated());
        Assert.assertNotNull(createdRatingEngine.getUpdated());
        Assert.assertNotNull(createdRatingEngine.getDisplayName());
        Assert.assertNotNull(createdRatingEngine.getNote());

        Assert.assertEquals(createdRatingEngine.getCreatedBy(), CREATED_BY);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        Assert.assertTrue(MapUtils.isEmpty(createdRatingEngine.getCountsAsMap()));
        switch (createdRatingEngine.getType()) {
        case RULE_BASED:
            detailedAssertionForRuleBasedModel(createdRatingEngine);
            break;
        case CROSS_SELL:
            break;
        default:
            break;
        }

    }

    private void detailedAssertionForRuleBasedModel(RatingEngine createdRatingEngine) {
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        RuleBasedModel model = (RuleBasedModel) createdRatingEngine.getLatestIteration();
        Set<String> selectedAttributes = new HashSet<>(model.getSelectedAttributes());
        Assert.assertNotNull(selectedAttributes);
        Assert.assertFalse(selectedAttributes.isEmpty());
        Assert.assertNotNull(model.getRatingRule());
        TreeMap<String, Map<String, Restriction>> bucketRuleMap = model.getRatingRule().getBucketToRuleMap();
        Assert.assertNotNull(bucketRuleMap);
        Assert.assertFalse(bucketRuleMap.isEmpty());
        Arrays.asList(RatingBucketName.values()) //
                .forEach(b -> {
                    Assert.assertTrue(bucketRuleMap.containsKey(b.name()));
                    Map<String, Restriction> rulesMap = bucketRuleMap.get(b.name());
                    Assert.assertNotNull(rulesMap);
                    Assert.assertFalse(rulesMap.isEmpty());
                    List<Boolean> hasNonEmptyPrepopulatedRules = new ArrayList<>();
                    rulesMap.keySet() //
                            .forEach(t -> {
                                Assert.assertTrue(FrontEndQueryConstants.ACCOUNT_RESTRICTION.equals(t) //
                                        || FrontEndQueryConstants.CONTACT_RESTRICTION.equals(t));
                                Assert.assertTrue(FrontEndQueryConstants.ACCOUNT_RESTRICTION.equals(t) //
                                        || FrontEndQueryConstants.CONTACT_RESTRICTION.equals(t));
                                Restriction topRestriction = rulesMap.get(t);
                                if (topRestriction != null) {
                                    hasNonEmptyPrepopulatedRules.add(true);
                                    Assert.assertTrue(topRestriction instanceof LogicalRestriction);
                                    List<Restriction> unusedRestrictions = ((LogicalRestriction) topRestriction)
                                            .getRestrictions();
                                    Assert.assertFalse(unusedRestrictions.isEmpty());
                                    unusedRestrictions.stream().forEach(u -> {
                                        Assert.assertTrue(u instanceof BucketRestriction);
                                        BucketRestriction unusedRestriction = (BucketRestriction) u;
                                        Assert.assertTrue(unusedRestriction.getIgnored());
                                        Assert.assertTrue(
                                                selectedAttributes.contains(unusedRestriction.getAttr().toString()),
                                                String.format(
                                                        "selectedAttributes = %s, unusedRestriction.getAttr() = %s, unusedRestriction = %s",
                                                        JsonUtils.serialize(selectedAttributes),
                                                        unusedRestriction.getAttr().toString(),
                                                        JsonUtils.serialize(unusedRestriction)));
                                    });
                                }
                            });
                    Assert.assertFalse(hasNonEmptyPrepopulatedRules.isEmpty());
                });
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreate" })
    public void testGet() {
        // test get a list
        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 2);

        // test get a list of ratingEngine summaries
        List<RatingEngineSummary> summaries = getAllRatingEngineSummaries();
        log.info("ratingEngineSummaries is " + summaries);
        Assert.assertNotNull(summaries);
        Assert.assertEquals(summaries.size(), 2);
        Assert.assertEquals(summaries.get(0).getSegmentDisplayName(), SEGMENT_NAME);
        Assert.assertEquals(summaries.get(0).getSegmentName(), rbRatingEngine.getSegment().getName());

        // test get list of ratingEngine summaries filtered by type and status
        summaries = getAllRatingEngineSummaries(null, null);
        Assert.assertEquals(summaries.size(), 2);
        summaries = getAllRatingEngineSummaries(RatingEngineType.CROSS_SELL.name(), null);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(), null);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 2);
        summaries = getAllRatingEngineSummaries(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(), RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(), RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(RatingEngineType.CROSS_SELL.name(), RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummaries(RatingEngineType.CROSS_SELL.name(), RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        // test Rating Attributes in Redshift
        summaries = getAllRatingEngineSummaries(RatingEngineType.CROSS_SELL.name(), RatingEngineStatus.INACTIVE.name(),
                true);
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummaries(RatingEngineType.CROSS_SELL.name(), RatingEngineStatus.INACTIVE.name(),
                false);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(), RatingEngineStatus.INACTIVE.name(),
                true);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(), RatingEngineStatus.INACTIVE.name(),
                false);
        Assert.assertEquals(summaries.size(), 1);

        // test basic find For RuleBased
        assertFindRatingEngine(rbRatingEngineId, RatingEngineType.RULE_BASED);
        // test basic find For AIBased
        assertFindRatingEngine(aiRatingEngineId, RatingEngineType.CROSS_SELL);
    }

    protected RatingEngine assertFindRatingEngine(String ratingEngineId, RatingEngineType type) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(ratingEngine.getId(), ratingEngineId);
        MetadataSegment segment = ratingEngine.getSegment();
        Assert.assertNotNull(segment);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);
        Assert.assertEquals(ratingEngine.getType(), type);
        String createdRatingEngineStr = ratingEngine.getId();
        ratingEngine = getRatingEngineById(ratingEngineId, true, true);
        Assert.assertNotNull(ratingEngine);
        log.info("String is " + createdRatingEngineStr);

        // test rating engine note creation
        List<RatingEngineNote> ratingEngineNotes = ratingEngineNoteService.getAllByRatingEngineId(rbRatingEngineId);
        Assert.assertNotNull(ratingEngineNotes);
        Assert.assertEquals(ratingEngineNotes.size(), 1);
        Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE);

        switch (type) {
        case RULE_BASED:
            RatingModel rm = ratingEngine.getLatestIteration();
            Assert.assertNotNull(rm);
            Assert.assertTrue(rm instanceof RuleBasedModel);
            Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                    RatingRule.DEFAULT_BUCKET_NAME);
            break;
        case CROSS_SELL:
            List<RatingModel> ratingModels = getRatingModelsByRatingEngineId(ratingEngineId);
            Assert.assertNotNull(ratingModels);
            Assert.assertEquals(ratingModels.size(), 1);
            rm = ratingModels.get(0);
            Assert.assertTrue(rm instanceof AIModel);
            break;
        }

        log.info("Rating Engine after findById is " + ratingEngine.toString());
        return ratingEngine;
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testGet" })
    public void testUpdateRatingEngine() {
        updateRatingEngine(rbRatingEngine);
        updateRatingEngine(aiRatingEngine);

        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 2);

        List<RatingEngineSummary> summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 2);
        summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(), RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummaries(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummaries(RatingEngineType.CROSS_SELL.name(), RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        // test Rating Attributes in Redshift
        summaries = getAllRatingEngineSummaries(RatingEngineType.CROSS_SELL.name(), RatingEngineStatus.ACTIVE.name(),
                true);
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummaries(RatingEngineType.CROSS_SELL.name(), RatingEngineStatus.ACTIVE.name(),
                false);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(), RatingEngineStatus.ACTIVE.name(),
                true);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(RatingEngineType.RULE_BASED.name(), RatingEngineStatus.ACTIVE.name(),
                false);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummaries(null, RatingEngineStatus.ACTIVE.name(), true);
        Assert.assertEquals(summaries.size(), 1);
        Assert.assertEquals(summaries.get(0).getId(), rbRatingEngineId);

    }

    protected void updateRatingEngine(RatingEngine ratingEngine) {
        createdDate = ratingEngine.getCreated();
        updatedDate = ratingEngine.getUpdated();

        RatingEngine re = new RatingEngine();
        re.setId(ratingEngine.getId());
        re.setScoringIteration(ratingEngine.getLatestIteration());
        createOrUpdate(re);

        // test update rating engine
        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        ratingEngine.setNote(RATING_ENGINE_NEW_NOTE);
        RatingEngine updatedRatingEngine = createOrUpdate(ratingEngine);
        Assert.assertEquals(RATING_ENGINE_NAME, updatedRatingEngine.getDisplayName());
        Assert.assertTrue(updatedRatingEngine.getUpdated().after(updatedDate));
        log.info("Created date is " + createdDate);
        log.info("The create date for the newly updated one is " + updatedRatingEngine.getCreated());

        // test rating engine note update
        List<RatingEngineNote> ratingEngineNotes = ratingEngineNoteService.getAllByRatingEngineId(ratingEngine.getId());
        Assert.assertNotNull(ratingEngineNotes);
        Assert.assertEquals(ratingEngineNotes.size(), 2);
        Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE);
        Assert.assertEquals(ratingEngineNotes.get(1).getNotesContents(), RATING_ENGINE_NEW_NOTE);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testUpdateRatingEngine" })
    public void testReplicate() {
        RatingEngine replicatedRule = ratingEngineService.replicateRatingEngine(rbRatingEngineId);
        assertReplicatedEngine(replicatedRule, rbRatingEngine);

        RatingEngine replicatedAI = ratingEngineService.replicateRatingEngine(aiRatingEngineId);
        assertReplicatedEngine(replicatedAI, aiRatingEngine);

        deactivateRatingEngine(replicatedRule.getId());
        deactivateRatingEngine(replicatedAI.getId());
        hardDeleteRatingEngine(replicatedRule.getId());
        hardDeleteRatingEngine(replicatedAI.getId());
    }

    private void assertReplicatedEngine(RatingEngine replicated, RatingEngine original) {
        Assert.assertNotNull(replicated);
        Assert.assertNotNull(original);

        if (original.getLatestIteration() != null) {
            Assert.assertNotNull(replicated.getLatestIteration());
            Assert.assertNotEquals(replicated.getLatestIteration().getId(), original.getLatestIteration().getId());
        }

        if (original.getLatestIteration() != null) {
            Assert.assertNotNull(replicated.getLatestIteration());
            Assert.assertNotEquals(replicated.getLatestIteration().getId(), original.getLatestIteration().getId());
        }

        Assert.assertNull(replicated.getScoringIteration());
        Assert.assertNull(replicated.getPublishedIteration());

        Assert.assertNotEquals(replicated.getId(), original.getId());
        Assert.assertNotEquals(replicated.getDisplayName(), original.getDisplayName());
        Assert.assertEquals(replicated.getType(), original.getType());

        Assert.assertTrue(CollectionUtils.isEmpty(replicated.getRatingEngineNotes()));

        if (original.getSegment() != null) {
            Assert.assertNotNull(replicated.getSegment());
            Assert.assertEquals(replicated.getSegment().getName(), original.getSegment().getName());
        }
        Assert.assertEquals(replicated.getType(), original.getType());
        Assert.assertEquals(replicated.getStatus(), RatingEngineStatus.INACTIVE);
        Assert.assertFalse(replicated.getDeleted());
        Assert.assertEquals(replicated.getAdvancedRatingConfigStr(), original.getAdvancedRatingConfigStr());
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testReplicate" })
    public void testCreateModelIteration() {
        RuleBasedModel ruleBasedModel = createRuleBasedModel();
        boolean exception = false;
        try {
            ratingEngineService.createModelIteration(rbRatingEngine, ruleBasedModel);
        } catch (Exception e) {
            exception = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_40038);
        }
        assertTrue(exception);

        AIModel aiModel = createAIModel(aiRatingEngine.getLatestIteration());
        aiModel.setDerivedFromRatingModel(aiRatingEngine.getLatestIteration().getId());

        AIModel derivedModel = (AIModel) aiRatingEngine.getLatestIteration();
        derivedModel.setModelingJobStatus(JobStatus.COMPLETED);
        ratingEngineService.updateRatingModel(aiRatingEngine.getId(), derivedModel.getId(), derivedModel);

        aiModel = (AIModel) ratingEngineService.createModelIteration(aiRatingEngine, aiModel);

        List<RatingModel> ratingModels = getRatingModelsByRatingEngineId(aiRatingEngineId);
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 2);
        log.info("Assert Iteration : " + aiModel.getIteration());
        Assert.assertEquals(aiModel.getIteration(), 2);
        Assert.assertEquals(aiModel.getCreatedBy(), CREATED_BY);
        Assert.assertEquals(aiRatingEngine.getLatestIteration().getId(), aiModel.getId());
        Assert.assertEquals(aiRatingEngine.getId(), aiModel.getRatingEngine().getId());
    }

    protected RuleBasedModel createRuleBasedModel() {
        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setId(RuleBasedModel.generateIdStr());
        return ruleBasedModel;
    }

    protected AIModel createAIModel(RatingModel ratingModel) {
        AIModel aiModel = new AIModel();
        aiModel.setId(AIModel.generateIdStr());
        aiModel.setRatingEngine(aiRatingEngine);
        ((RatingModel) aiModel).setCreatedBy(CREATED_BY);
        ((RatingModel) aiModel).setUpdatedBy(CREATED_BY);
        aiModel.setAdvancedModelingConfig(new CrossSellModelingConfig());
        return aiModel;
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreateModelIteration" })
    public void testDelete() {
        // update Rating Engine to be inactive for deletion
        deactivateRatingEngine(rbRatingEngineId);
        deactivateRatingEngine(aiRatingEngineId);

        // Soft Delete Rule Based Rating Engine
        deleteSoftRatingEngine(rbRatingEngineId);
        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);

        // Revert Delete Rule Based Rating Engine
        testRevertDeleteRatingEngine(rbRatingEngineId);
        ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 2);

        // Soft Delete Rule Based Rating Engine & Ai Rating Engine
        deleteSoftRatingEngine(rbRatingEngineId);
        deleteSoftRatingEngine(aiRatingEngineId);

        ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);

        ratingEngineList = getAllDeletedRatingEngines();
        Assert.assertEquals(ratingEngineList.size(), 2);
        Assert.assertTrue(ratingEngineList.stream().allMatch(RatingEngine::getDeleted));

        hardDeleteRatingEngine(rbRatingEngineId);
        hardDeleteRatingEngine(aiRatingEngineId);
        ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = getAllDeletedRatingEngines();
        Assert.assertEquals(ratingEngineList.size(), 0);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testDelete" })
    public void testCyclicDependency() {
        MetadataSegment segment1 = createSegment(SEGMENT_NAME + "1");
        MetadataSegment segment2 = createSegment(SEGMENT_NAME + "2");

        RatingEngine ratingEngine1 = createRatingEngine(segment2);
        RatingEngine ratingEngine2 = createRatingEngine(segment1);

        setRestriction(segment1, ratingEngine1);
        boolean exception = false;
        try {
            setRestriction(segment2, ratingEngine2);
        } catch (Exception e) {
            exception = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_40041);
        }
        assertTrue(exception);

        ratingEngine2.setNote("Test");
        createOrUpdate(ratingEngine2);
    }

    protected void deleteSoftRatingEngine(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        String createdRatingEngineStr = ratingEngine.toString();
        log.info("Before delete, getting complete Rating Engine : " + createdRatingEngineStr);

        // test soft delete
        deleteById(ratingEngine.getId(), false, CREATED_BY);
        ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        Assert.assertNotNull(ratingEngine);
        Assert.assertTrue(ratingEngine.getDeleted());
    }

    protected void hardDeleteRatingEngine(String ratingEngineId) {
        deleteById(ratingEngineId);
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        Assert.assertNull(ratingEngine);
    }

    protected void testRevertDeleteRatingEngine(String ratingEngineId) {
        revertDelete(ratingEngineId);
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        Assert.assertNotNull(ratingEngine);
        Assert.assertFalse(ratingEngine.getDeleted());
    }

    protected List<RatingEngineSummary> getAllRatingEngineSummaries() {
        return ratingEngineService.getAllRatingEngineSummaries();
    }

    protected List<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId) {
        return ratingEngineService.getRatingModelsByRatingEngineId(ratingEngineId);
    }

    protected List<RatingEngineSummary> getAllRatingEngineSummaries(String type, String status) {
        return ratingEngineService.getAllRatingEngineSummaries(type, status);
    }

    protected List<RatingEngineSummary> getAllRatingEngineSummaries(String type, String status,
            Boolean onlyInRedshift) {
        return ratingEngineService.getAllRatingEngineSummaries(type, status, onlyInRedshift);
    }

    protected RatingEngine getRatingEngineById(String ratingEngineId, boolean populateRefreshedDate,
            boolean populateActiveModel) {
        return ratingEngineService.getRatingEngineById(ratingEngineId, populateRefreshedDate, populateActiveModel);
    }

    protected RatingEngine createOrUpdate(RatingEngine ratingEngine) {
        return ratingEngineService.createOrUpdate(ratingEngine);
    }

    protected void deleteById(String ratingEngineId) {
        ratingEngineService.deleteById(ratingEngineId);
    }

    protected void deleteById(String ratingEngineId, boolean hardDelete, String actionInitiator) {
        ratingEngineService.deleteById(ratingEngineId, hardDelete, actionInitiator);
    }

    protected List<RatingEngine> getAllDeletedRatingEngines() {
        return ratingEngineService.getAllDeletedRatingEngines();
    }

    protected void revertDelete(String ratingEngineId) {
        ratingEngineService.revertDelete(ratingEngineId);
    }

    protected void deactivateRatingEngine(String ratingEngineId) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngineId);
        ratingEngine.setStatus(RatingEngineStatus.INACTIVE);
        createOrUpdate(ratingEngine);
    }

    protected MetadataSegment createSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(segmentName);
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), segment);

        try {
            Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
        }

        return segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), createdSegment.getName());
    }

    protected RatingEngine createRatingEngine(MetadataSegment segment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(segment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE);

        return createOrUpdate(ratingEngine);
    }

    protected void setRestriction(MetadataSegment segment, RatingEngine ratingEngine) {
        Restriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Rating, ratingEngine.getId()), Bucket.notNullBkt());
        segment.setAccountRestriction(accountRestriction);
        segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), segment);
    }
}
