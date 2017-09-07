package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class RatingEngineResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResourceDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME_1 = "Rating Engine 1";
    private static final String RATING_ENGINE_NOTE_1 = "This is a Rating Engine that covers North America market";
    @SuppressWarnings("unused")
    private static final String RATING_ENGINE_NAME_2 = "Rating Engine 1";
    @SuppressWarnings("unused")
    private static final String RATING_ENGINE_NOTE_2 = "This is a Rating Engine that covers East Asia market";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final String ATTR1 = "Employ Number";
    private static final String ATTR2 = "Revenue";
    private static final String ATTR3 = "Has Cisco WebEx";

    @Autowired
    private MetadataSegmentService metadataSegmentService;

    private MetadataSegment segment;

    private RatingEngine re1;
    private RatingEngine re2;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
        MultiTenantContext.setTenant(mainTestTenant);

        segment = new MetadataSegment();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction());
        segment.setContactFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = metadataSegmentService.createOrUpdateSegment(segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = metadataSegmentService.getSegmentByName(createdSegment.getName(), false);
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));

        re1 = new RatingEngine();
        re1.setSegment(retrievedSegment);
        re1.setCreatedBy(CREATED_BY);
        re1.setType(RatingEngineType.RULE_BASED);
        re2 = new RatingEngine();
        re2.setSegment(retrievedSegment);
        re2.setCreatedBy(CREATED_BY);
        re2.setType(RatingEngineType.RULE_BASED);
    }

    @Test(groups = "deployment")
    public void testCreate() {
        RatingEngine createdRe1 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines", re1,
                RatingEngine.class);

        RatingEngine createdRe2 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines", re2,
                RatingEngine.class);

        Assert.assertNotNull(createdRe1);
        Assert.assertNotNull(createdRe2);
        re1.setId(createdRe1.getId());
        re2.setId(createdRe2.getId());
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
    public void testGet() {
        // test get all rating engine summary list
        List<?> ratingEngineSummarieObjects = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines",
                List.class);
        List<RatingEngineSummary> ratingEngineSummaries = JsonUtils.convertList(ratingEngineSummarieObjects,
                RatingEngineSummary.class);
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

        // test get specific rating engine
        RatingEngine ratingEngine = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId(), RatingEngine.class);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(ratingEngine.getId(), re1.getId());
        MetadataSegment segment = ratingEngine.getSegment();
        Assert.assertNotNull(segment);
        log.info("After loading, ratingEngine is " + ratingEngine);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);

        Set<RatingModel> ratingModels = ratingEngine.getRatingModels();
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Iterator<RatingModel> it = ratingModels.iterator();
        RatingModel rm = it.next();
        Assert.assertTrue(rm instanceof RuleBasedModel);
        Assert.assertEquals(rm.getIteration(), 1);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGet" })
    public void testUpdate() {
        // test update rating engine
        re1.setDisplayName(RATING_ENGINE_NAME_1);
        re1.setNote(RATING_ENGINE_NOTE_1);
        RatingEngine ratingEngine = restTemplate.postForObject(getRestAPIHostPort() + "/pls/ratingengines", re1,
                RatingEngine.class);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(RATING_ENGINE_NAME_1, ratingEngine.getDisplayName());
        Assert.assertEquals(RATING_ENGINE_NOTE_1, ratingEngine.getNote());
        Assert.assertEquals(re1.getId(), ratingEngine.getId());

        @SuppressWarnings("unchecked")
        List<RatingEngineSummary> ratingEngineSummaries = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines", List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 2);

        ratingEngine = restTemplate.getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId(),
                RatingEngine.class);
        Assert.assertEquals(RATING_ENGINE_NAME_1, ratingEngine.getDisplayName());
        Assert.assertEquals(RATING_ENGINE_NOTE_1, ratingEngine.getNote());
        Assert.assertEquals(re1.getId(), ratingEngine.getId());

        // test update rule based model
        Set<?> ratingModelObjects = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId() + "/ratingmodels", Set.class);
        Set<RatingModel> ratingModels = JsonUtils.convertSet(ratingModelObjects, RatingModel.class);
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
        ratingRule.setDefaultBucketName(RuleBucketName.D.getName());
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setSelectedAttributes(generateSeletedAttributes());
        rm = restTemplate.postForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId() + "/ratingmodels/" + ratingModelId,
                ruleBasedModel, RatingModel.class);
        Assert.assertNotNull(rm);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(), RuleBucketName.D.getName());
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR1));
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR2));
        Assert.assertTrue(((RuleBasedModel) rm).getSelectedAttributes().contains(ATTR3));

    }

    private List<String> generateSeletedAttributes() {
        List<String> selectedAttributes = new ArrayList<>();
        selectedAttributes.add(ATTR1);
        selectedAttributes.add(ATTR2);
        selectedAttributes.add(ATTR3);
        return selectedAttributes;
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re1.getId());
        restTemplate.delete(getRestAPIHostPort() + "/pls/ratingengines/" + re2.getId());
        List<RatingEngineSummary> ratingEngineSummaries = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/ratingengines", List.class);
        Assert.assertNotNull(ratingEngineSummaries);
        Assert.assertEquals(ratingEngineSummaries.size(), 0);
    }

}
