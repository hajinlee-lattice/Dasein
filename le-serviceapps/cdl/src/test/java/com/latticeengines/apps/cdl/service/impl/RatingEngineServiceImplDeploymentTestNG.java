package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RatingEngineNoteService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
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

    @BeforeClass(groups = "deployment")
    public void setup() throws KeyManagementException, NoSuchAlgorithmException, IOException {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(),
                constructSegment(SEGMENT_NAME));
        Assert.assertNotNull(createdSegment);
        reTestSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", reTestSegment.getName()));
    }

    @Test(groups = "deployment")
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
        ratingEngine.setType(type);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        // test basic creation
        ratingEngine = createOrUpdate(ratingEngine, mainTestTenant.getId());

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
        Assert.assertNotNull(createdRatingEngine.getActiveModel());
        Assert.assertTrue(MapUtils.isEmpty(createdRatingEngine.getCountsAsMap()));
        switch (createdRatingEngine.getType()) {
        case RULE_BASED:
            Assert.assertNotNull(createdRatingEngine.getActiveModelPid());
            break;
        case CROSS_SELL:
            break;
        default:
            break;
        }
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
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
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(null, null);
        Assert.assertEquals(summaries.size(), 2);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.CROSS_SELL.name(), null);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.RULE_BASED.name(), null);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 2);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        // test Rating Attributes in Redshift
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.INACTIVE.name(), true);
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.INACTIVE.name(), false);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name(), true);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name(), false);
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
        String createdRatingEngineStr = ratingEngine.getId().toString();
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
            RatingModel rm = ratingEngine.getActiveModel();
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

    @Test(groups = "deployment", dependsOnMethods = { "testGet" })
    public void testUpdateRatingEngine() {
        updateRatingEngine(rbRatingEngine);
        updateRatingEngine(aiRatingEngine);

        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 2);

        List<RatingEngineSummary> summaries = getAllRatingEngineSummariesWithTypeAndStatus(
                RatingEngineType.RULE_BASED.name(), RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 2);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        // test Rating Attributes in Redshift
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.ACTIVE.name(), true);
        Assert.assertEquals(summaries.size(), 0);
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(RatingEngineType.CROSS_SELL.name(),
                RatingEngineStatus.ACTIVE.name(), false);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name(), true);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name(), false);
        Assert.assertEquals(summaries.size(), 1);
        summaries = getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(null, RatingEngineStatus.ACTIVE.name(),
                true);
        Assert.assertEquals(summaries.size(), 1);
        Assert.assertEquals(summaries.get(0).getId(), rbRatingEngineId);

    }

    protected void updateRatingEngine(RatingEngine ratingEngine) {
        createdDate = ratingEngine.getCreated();
        updatedDate = ratingEngine.getUpdated();

        // test update rating engine
        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        ratingEngine.setNote(RATING_ENGINE_NEW_NOTE);
        RatingEngine updatedRatingEngine = createOrUpdate(ratingEngine, mainTestTenant.getId());
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

    @Test(groups = "deployment", dependsOnMethods = { "testUpdateRatingEngine" })
    public void testDelete() {
        deleteRatingEngine(rbRatingEngineId);
        deleteRatingEngine(aiRatingEngineId);

        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);
    }

    protected void deleteRatingEngine(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        String createdRatingEngineStr = ratingEngine.toString();
        log.info("Before delete, getting complete Rating Engine : " + createdRatingEngineStr);

        // test delete
        deleteById(ratingEngine.getId());
        ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        Assert.assertNull(ratingEngine);
    }

    protected List<RatingEngineSummary> getAllRatingEngineSummaries() {
        return ratingEngineService.getAllRatingEngineSummaries();
    }

    protected List<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId) {
        return ratingEngineService.getRatingModelsByRatingEngineId(ratingEngineId);
    }

    protected List<RatingEngineSummary> getAllRatingEngineSummariesWithTypeAndStatus(String type, String status) {
        return ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(type, status);
    }

    protected List<RatingEngineSummary> getAllRatingEngineSummariesWithTypeStatusAndRedshiftStatus(String type,
            String status, Boolean onlyInRedshift) {
        return ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatusInRedShift(type, status, onlyInRedshift);
    }

    protected RatingEngine getRatingEngineById(String ratingEngineId, boolean populateRefreshedDate,
            boolean populateActiveModel) {
        return ratingEngineService.getRatingEngineById(ratingEngineId, populateRefreshedDate, populateActiveModel);
    }

    protected RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId) {
        return ratingEngineService.createOrUpdate(ratingEngine, tenantId);
    }

    protected void deleteById(String ratingEngineId) {
        ratingEngineService.deleteById(ratingEngineId);
    }
}
