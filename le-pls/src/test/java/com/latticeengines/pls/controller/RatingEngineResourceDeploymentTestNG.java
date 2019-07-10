package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.CrossSellRatingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.CustomEventRatingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class RatingEngineResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResourceDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME_1 = "Rating Engine 1";
    private static final String RATING_ENGINE_NOTE_1 = "This is a Rating Engine that covers North America market";
    @SuppressWarnings("unused")
    private static final String RATING_ENGINE_NAME_2 = "Rating Engine 1";
    private static final String RATING_ENGINE_NOTE_2 = "This is a Rating Engine that covers East Asia market";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final String ATTR1 = "Employ Number";
    private static final String ATTR2 = "Revenue";
    private static final String ATTR3 = "Has Cisco WebEx";

    private static final String LDC_NAME = "LDC_Name";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    private final boolean shouldCreateActionWithRatingEngine1 = true;
    private final boolean shouldCreateActionWithRatingEngine2 = false;

    @Autowired
    private MetadataSegmentService metadataSegmentService;

    @Autowired
    private ActionProxy actionProxy;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

    @Autowired
    private CDLTestDataService cdlTestDataService;

    private MetadataSegment segment;

    private RatingEngine re1;
    private RatingEngine re2;
    private RatingEngine re3;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
        MultiTenantContext.setTenant(mainTestTenant);
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
        segment = constructSegment(SEGMENT_NAME);
        MetadataSegment createdSegment = metadataSegmentService.createOrUpdateSegment(segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = metadataSegmentService.getSegmentByName(createdSegment.getName(), false);
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));

        re1 = createRuleBasedRatingEngine(retrievedSegment);
        re2 = createAIRatingEngine(retrievedSegment, RatingEngineType.CROSS_SELL);
        re3 = createAIRatingEngine(retrievedSegment, RatingEngineType.CUSTOM_EVENT);
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
        testCreate(re3);
        testRatingEngineNoteCreation(re3, false);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testGet() {
        // test get all rating engine summary list
        List<?> ratingEngineSummarieObjects = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines",
                List.class);
        List<RatingEngineSummary> ratingEngineSummaries = JsonUtils.convertList(ratingEngineSummarieObjects,
                RatingEngineSummary.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 3);
        log.info("ratingEngineSummaries is " + ratingEngineSummaries);
        String id1 = re1.getId();
        String id2 = re2.getId();
        String id3 = re3.getId();
        RatingEngineSummary possibleRatingEngineSummary1 = null;
        RatingEngineSummary possibleRatingEngineSummary2 = null;
        RatingEngineSummary possibleRatingEngineSummary3 = null;
        for (RatingEngineSummary r : ratingEngineSummaries) {
            if (r.getId().equals(id1)) {
                possibleRatingEngineSummary1 = r;
            } else if (r.getId().equals(id2)) {
                possibleRatingEngineSummary2 = r;
            } else if (r.getId().equals(id3)) {
                possibleRatingEngineSummary3 = r;
            }
            if (r.getType() != RatingEngineType.RULE_BASED) {
                Assert.assertNotNull(r.getAdvancedRatingConfig());
            }
        }
        Assert.assertNotNull(possibleRatingEngineSummary1);
        Assert.assertNotNull(possibleRatingEngineSummary2);
        Assert.assertNotNull(possibleRatingEngineSummary3);
        Assert.assertEquals(possibleRatingEngineSummary1.getSegmentDisplayName(),
                possibleRatingEngineSummary2.getSegmentDisplayName());
        Assert.assertEquals(possibleRatingEngineSummary1.getSegmentDisplayName(),
                possibleRatingEngineSummary3.getSegmentDisplayName());
        Assert.assertEquals(possibleRatingEngineSummary1.getSegmentDisplayName(), SEGMENT_NAME);

        // test get all rating engine summary list filtered by type and status
        ratingEngineSummarieObjects = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines?status=INACTIVE", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 2);
        ratingEngineSummarieObjects = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines?status=ACTIVE", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 1);
        ratingEngineSummarieObjects = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines?status=INACTIVE&type=RULE_BASED", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 0);
        ratingEngineSummarieObjects = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines?status=ACTIVE&type=RULE_BASED", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 1);
        ratingEngineSummarieObjects = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines?type=CROSS_SELL", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 1);
        ratingEngineSummarieObjects = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines?type=CUSTOM_EVENT", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 1);
        // test Rating Attribute in Redshift
        ratingEngineSummarieObjects = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines?status=ACTIVE&type=RULE_BASED&publishedratingsonly=true",
                List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 1);
        ratingEngineSummarieObjects = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines?status=ACTIVE&type=RULE_BASED&publishedratingsonly=false",
                List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 1);
        ratingEngineSummarieObjects = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines?status=ACTIVE&publishedratingsonly=true", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 1);
        ratingEngineSummarieObjects = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines?type=CROSS_SELL&publishedratingsonly=true", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 0);
        ratingEngineSummarieObjects = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines?type=CUSTOM_EVENT&publishedratingsonly=true", List.class);
        Assert.assertEquals(ratingEngineSummarieObjects.size(), 0);

        // test get specific rating engine
        RatingEngine ratingEngine = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId(), RatingEngine.class);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(ratingEngine.getId(), re1.getId());
        MetadataSegment segment = ratingEngine.getSegment();
        Assert.assertNotNull(segment);
        log.info("After loading, ratingEngine is " + ratingEngine);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);

        RatingModel ratingModel = ratingEngine.getLatestIteration();
        Assert.assertNotNull(ratingModel);
        Assert.assertTrue(ratingModel instanceof RuleBasedModel);
        Assert.assertEquals(((RuleBasedModel) ratingModel).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "testGet" })
    public void testUpdate() {
        // test update rating engine
        re1.setDisplayName(RATING_ENGINE_NAME_1);
        re1.setStatus(RatingEngineStatus.INACTIVE);
        re1.setNote(RATING_ENGINE_NOTE_2);
        RatingEngine ratingEngine = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines", re1,
                RatingEngine.class);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(ratingEngine.getDisplayName(), RATING_ENGINE_NAME_1);
        Assert.assertEquals(re1.getId(), ratingEngine.getId());
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.INACTIVE);

        // test update rating engine note
        List<RatingEngineNote> ratingEngineNotes = ratingEngineProxy.getAllNotes(mainTestTenant.getId(),
                ratingEngine.getId());
        Assert.assertNotNull(ratingEngineNotes);
        Assert.assertEquals(ratingEngineNotes.size(), 2);
        Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE_1);
        Assert.assertEquals(ratingEngineNotes.get(1).getNotesContents(), RATING_ENGINE_NOTE_2);

        List<RatingEngineSummary> ratingEngineSummaries = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines", List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 3);

        ratingEngine = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId(),
                RatingEngine.class);
        Assert.assertEquals(RATING_ENGINE_NAME_1, ratingEngine.getDisplayName());
        Assert.assertEquals(ratingEngine.getId(), re1.getId());
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.INACTIVE);

        ratingEngineSummaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines?status=ACTIVE",
                List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        ratingEngineSummaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines?status=INACTIVE",
                List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 3);
        ratingEngineSummaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines?type=CROSS_SELL",
                List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines?type=RULE_BASED",
                List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        // test Rating Attribute in Redshift
        ratingEngineSummaries = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines?publishedratingsonly=true", List.class);
        Assert.assertEquals(ratingEngineSummaries.size(), 1);
        ratingEngineSummaries = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines?publishedratingsonly=false", List.class);
        Assert.assertEquals(ratingEngineSummaries.size(), 3);

        // test update rule based model
        List<?> ratingModelObjects = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId() + "/ratingmodels", List.class);
        List<RatingModel> ratingModels = JsonUtils.convertList(ratingModelObjects, RatingModel.class);
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
        rm = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId() + "/ratingmodels/" + ratingModelId,
                RatingModel.class);
        Assert.assertNotNull(rm);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RatingBucketName.D.getName());
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setSelectedAttributes(generateSeletedAttributes());
        rm = restTemplate.postForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId() + "/ratingmodels/" + ratingModelId,
                ruleBasedModel, RatingModel.class);
        Assert.assertNotNull(rm);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(), RatingBucketName.D.getName());
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR1));
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR2));
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR3));
        assertRuleBasedModelUpdateAction(ratingEngine, ratingModelId);

        // update only the selected attributes
        ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setSelectedAttributes(generateSeletedAttributes());
        rm = restTemplate.postForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId() + "/ratingmodels/" + ratingModelId,
                ruleBasedModel, RatingModel.class);
        log.info("Second time rm is " + rm);
        Assert.assertNotNull(rm);
        assertSecondTimeRatingModelUpdateDoesNotGenerateAction();
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

    private RatingEngine createRuleBasedRatingEngine(MetadataSegment retrievedSegment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE_1);
        if (shouldCreateActionWithRatingEngine1) {
            ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        }
        return ratingEngine;
    }

    private RatingEngine createAIRatingEngine(MetadataSegment retrievedSegment, RatingEngineType type) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(type);
        if (shouldCreateActionWithRatingEngine2) {
            ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        }
        return ratingEngine;
    }

    private void testCreate(RatingEngine re) {
        RatingEngine createdRe = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines", re,
                RatingEngine.class);
        Assert.assertNotNull(createdRe);
        re.setId(createdRe.getId());
        Assert.assertNotNull(createdRe.getLatestIteration());
        RatingEngine retrievedRe = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + createdRe.getId(), RatingEngine.class);

        if (retrievedRe.getLatestIteration() instanceof RuleBasedModel) {
            Assert.assertNotNull(retrievedRe.getLatestIteration());
            RuleBasedModel ruModel = (RuleBasedModel) retrievedRe.getLatestIteration();
            Assert.assertNotNull(ruModel);
            Assert.assertNotNull(ruModel.getSelectedAttributes());
            Assert.assertTrue(ruModel.getSelectedAttributes().size() > 0);
            if (shouldCreateActionWithRatingEngine1) {
                assertRatingEngineActivationAction(createdRe);
            }
        } else if (retrievedRe.getLatestIteration() instanceof AIModel) {
            AIModel aiModel = (AIModel) retrievedRe.getLatestIteration();
            Assert.assertNotNull(aiModel);
            Assert.assertNotNull(retrievedRe.getAdvancedRatingConfig());
            Assert.assertNotNull(aiModel.getAdvancedModelingConfig());

            if (retrievedRe.getType() == RatingEngineType.CROSS_SELL) {
                Assert.assertEquals(retrievedRe.getAdvancedRatingConfig().getClass(), CrossSellRatingConfig.class);
                Assert.assertEquals(aiModel.getAdvancedModelingConfig().getClass(), CrossSellModelingConfig.class);
            } else if (retrievedRe.getType() == RatingEngineType.CUSTOM_EVENT) {
                Assert.assertEquals(retrievedRe.getAdvancedRatingConfig().getClass(), CustomEventRatingConfig.class);
                Assert.assertEquals(aiModel.getAdvancedModelingConfig().getClass(), CustomEventModelingConfig.class);
            }
        }
    }

    private List<String> generateSeletedAttributes() {
        List<String> selectedAttributes = new ArrayList<>();
        selectedAttributes.add(ATTR1);
        selectedAttributes.add(ATTR2);
        selectedAttributes.add(ATTR3);
        return selectedAttributes;
    }

    private void assertRatingEngineActivationAction(RatingEngine ratingEngine) {
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.ACTIVE);
        String tenantId = MultiTenantContext.getShortTenantId();
        List<Action> actions = actionProxy.getActions(tenantId);
        Assert.assertEquals(actions.size(), 1);
        Action action = actions.get(0);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);

        // TODO - anoop and yunlong to enable it
        // Assert.assertEquals(action.getActionInitiator(),
        // ratingEngine.getCreatedBy());

        Assert.assertNotNull(action.getDescription());
        log.info("RatingEngineActivationAction description is " + action.getDescription());
    }

    private void assertRuleBasedModelUpdateAction(RatingEngine ratingEngine, String ratingModelId) {
        String tenantId = MultiTenantContext.getShortTenantId();
        List<Action> actions = actionProxy.getActions(tenantId);
        Assert.assertEquals(actions.size(), 2);
        Action action = actions.get(1);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);

        // TODO - anoop and yunlong to enable it
        // Assert.assertEquals(action.getActionInitiator(),
        // ratingEngine.getCreatedBy());

        Assert.assertNotNull(action.getDescription());
        log.info("RuleBasedModelUpdateAction description is " + action.getDescription());
    }

    private void assertSecondTimeRatingModelUpdateDoesNotGenerateAction() {
        String tenantId = MultiTenantContext.getShortTenantId();
        List<Action> actions = actionProxy.getActions(tenantId);
        Assert.assertEquals(actions.size(), 2);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "testUpdate" })
    public void testUpdateNullSegment() {
        re2.setSegment(null);
        re2 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines?unlink-segment=false", re2,
                RatingEngine.class);
        Assert.assertNotNull(re2);
        Assert.assertNotNull(re2.getSegment());

        re2.setSegment(null);
        re2 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines?unlink-segment=true", re2,
                RatingEngine.class);
        Assert.assertNotNull(re2);
        Assert.assertNotNull(re2.getSegment());

        re3.setSegment(null);
        re3 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines?unlink-segment=false", re3,
                RatingEngine.class);
        Assert.assertNotNull(re3);
        Assert.assertNotNull(re3.getSegment());

        re3.setSegment(null);
        re3 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines?unlink-segment=true", re3,
                RatingEngine.class);
        Assert.assertNotNull(re3);
        Assert.assertNull(re3.getSegment());

    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "testUpdateNullSegment" })
    public void testDelete() {
        // Soft Delete Rule Based Rating Engine
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId());
        List<RatingEngineSummary> ratingEngineSummaries = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines", List.class);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);

        // Revert Delete Rule Based Rating Engine
        restTemplate.put(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId() + "/revertdelete", null);
        ratingEngineSummaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines", List.class);
        Assert.assertEquals(ratingEngineSummaries.size(), 3);

        // Soft Delete Rule Based Rating Engine & AI Rating Engine
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId());
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re2.getId());
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re3.getId());
        ratingEngineSummaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines", List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        List<RatingEngine> ratingEngineList = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines/deleted", List.class);
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 3);
        RatingEngine re = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId(),
                RatingEngine.class);
        Assert.assertTrue(re.getDeleted());
        re = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re2.getId(), RatingEngine.class);
        Assert.assertTrue(re.getDeleted());
        re = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re3.getId(), RatingEngine.class);
        Assert.assertTrue(re.getDeleted());
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId() + "?hard-delete=true");
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re2.getId() + "?hard-delete=true");
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re3.getId() + "?hard-delete=true");
        ratingEngineSummaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines", List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
        ratingEngineList = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines/deleted", List.class);
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);
    }

    public static MetadataSegment constructSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        Restriction accountRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "LDC_Name"),
                Bucket.notNullBkt());
        segment.setAccountRestriction(accountRestriction);
        Bucket titleBkt = Bucket.valueBkt("Buyer");
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        segment.setContactRestriction(contactRestriction);
        segment.setDisplayName(segmentName);
        return segment;
    }

}
