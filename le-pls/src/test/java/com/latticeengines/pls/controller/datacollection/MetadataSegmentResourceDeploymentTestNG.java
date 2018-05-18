package com.latticeengines.pls.controller.datacollection;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.proxy.TestRatingEngineProxy;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.testframework.exposed.proxy.pls.TestMetadataSegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class MetadataSegmentResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private long initialAccountNum;
    private long initialContactNum;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private TestMetadataSegmentProxy testSegmentProxy;

    @Inject
    private TestRatingEngineProxy testRatingEngineProxy;

    @Inject
    private ActionService actionService;

    private String segmentName;
    private String ratingEngineId;
    private int actionNumber = 0;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        attachProtectedProxy(testSegmentProxy);
        attachProtectedProxy(testRatingEngineProxy);
        cdlTestDataService.populateData(mainTestTenant.getId());
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment")
    public void testCreate() {
        List<MetadataSegment> segments = testSegmentProxy.getSegments();
        Assert.assertEquals(segments.size(), 0);

        MetadataSegment newSegment = new MetadataSegment();
        segmentName = NamingUtils.uuid("Segment");
        final String displayName = "Test Segment";
        final String description = "The description";
        newSegment.setName(segmentName);
        newSegment.setDisplayName(displayName);
        newSegment.setDescription(description);

        Restriction accountRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.LDC_Name.name()).gte("B").build();
        Restriction contactRestriction = Restriction.builder() //
                .let(BusinessEntity.Contact, InterfaceName.ContactName.name()).lt("R").build();
        newSegment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        newSegment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        MetadataSegment returned = testSegmentProxy.createOrUpdate(newSegment);
        Assert.assertNotNull(returned);
        Assert.assertEquals(returned.getName(), segmentName);
        Assert.assertEquals(returned.getDisplayName(), displayName);
        Assert.assertEquals(returned.getDescription(), description);

        Assert.assertNotNull(returned.getAccountFrontEndRestriction());
        Assert.assertNotNull(returned.getContactFrontEndRestriction());

        Assert.assertNotNull(returned.getAccounts());
        initialAccountNum = returned.getAccounts();
        Assert.assertNotNull(returned.getContacts());
        initialContactNum = returned.getContacts();

        RatingEngine ratingEngine = createRuleBasedRatingEngine(returned);
        Assert.assertNotNull(ratingEngine);
        ratingEngineId = ratingEngine.getId();
        cdlTestDataService.mockRatingTable(mainTestTenant.getId(), Collections.singletonList(ratingEngineId),
                generateRatingCounts(ratingEngineId));
        ratingEngine = testRatingEngineProxy.getRatingEngine(ratingEngineId);
        Map<String, Long> ratingCounts = ratingEngine.getCountsAsMap();
        Assert.assertTrue(MapUtils.isNotEmpty(ratingCounts));
        String counts = JsonUtils.serialize(ratingCounts);
        Assert.assertEquals(ratingCounts.get(RatingBucketName.A.getName()), new Long(136), counts);
        Assert.assertEquals(ratingCounts.get(RatingBucketName.D.getName()), new Long(1018), counts);
        Assert.assertEquals(ratingCounts.get(RatingBucketName.F.getName()), new Long(88), counts);
    }

    private Map<String, Map<RatingBucketName, Long>> generateRatingCounts(String ratingEngineId) {
        Map<RatingBucketName, Long> coverage = new HashMap<>();
        coverage.put(RatingBucketName.A, 136L);
        coverage.put(RatingBucketName.D, 1018L);
        coverage.put(RatingBucketName.F, 88L);
        Map<String, Map<RatingBucketName, Long>> ratingCounts = ImmutableMap.of(ratingEngineId, coverage);
        return ratingCounts;
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testUpdate() {
        assertMetadataSegmentUpdateActionNotGen();
        MetadataSegment segment = testSegmentProxy.getSegment(segmentName);
        Assert.assertNotNull(segment);
        Assert.assertEquals(segment.getAccounts(), new Long(initialAccountNum), JsonUtils.serialize(segment));
        Assert.assertEquals(segment.getContacts(), new Long(initialContactNum), JsonUtils.serialize(segment));

        Restriction accountRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.LDC_Name.name()).gte("F").build();
        Restriction contactRestriction = Restriction.builder() //
                .let(BusinessEntity.Contact, InterfaceName.ContactName.name()).lt("N").build();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        MetadataSegment returned = testSegmentProxy.createOrUpdate(segment);

        Assert.assertNotNull(returned.getAccountFrontEndRestriction());
        Assert.assertNotNull(returned.getContactFrontEndRestriction());

        Assert.assertNotEquals(returned.getAccounts(), new Long(initialAccountNum), JsonUtils.serialize(returned));
        Assert.assertNotEquals(returned.getContacts(), new Long(initialContactNum), JsonUtils.serialize(returned));
        assertMetadataSegmentUpdateAction();
        RatingEngine ratingEngine = testRatingEngineProxy.getRatingEngine(ratingEngineId);
        Assert.assertNotNull(ratingEngine);

        Map<String, Long> ratingCounts = ratingEngine.getCountsAsMap();
        Assert.assertTrue(MapUtils.isNotEmpty(ratingCounts));
        String counts = JsonUtils.serialize(ratingCounts);

        // TODO uncomment the assertion PLS-7360
        // Assert.assertEquals(ratingCounts.get(RatingBucketName.A.getName()),
        // new Long(104), counts);
        // Assert.assertEquals(ratingCounts.get(RatingBucketName.D.getName()),
        // new Long(709), counts);
        // Assert.assertEquals(ratingCounts.get(RatingBucketName.F.getName()),
        // new Long(67), counts);
    }

    private void assertMetadataSegmentUpdateActionNotGen() {
        List<Action> actions = actionService.findAll();
        Assert.assertEquals(actions.size(), actionNumber);
    }

    private void assertMetadataSegmentUpdateAction() {
        List<Action> actions = actionService.findAll();
        Assert.assertEquals(actions.size(), ++actionNumber);
        Action action = actions.get(actionNumber - 1);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.METADATA_SEGMENT_CHANGE);
    }

    private RatingEngine createRuleBasedRatingEngine(MetadataSegment segment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(segment);
        ratingEngine.setCreatedBy("test@lattice-engines.com");
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        RatingEngine retrievedRatingEngine = testRatingEngineProxy.createOrUpdate(ratingEngine);
        RatingEngine newEngine = testRatingEngineProxy.getRatingEngine(retrievedRatingEngine.getId());
        String modelId = newEngine.getActiveModel().getId();
        RuleBasedModel model = constructRuleModel(modelId);
        testRatingEngineProxy.updateRatingModel(newEngine.getId(), modelId, model);
        return testRatingEngineProxy.getRatingEngine(newEngine.getId());
    }

    private RuleBasedModel constructRuleModel(String modelId) {
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RatingBucketName.D.getName());

        Bucket bktA = Bucket.valueBkt("CALIFORNIA");
        Restriction resA = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "LDC_State"), bktA);
        ratingRule.setRuleForBucket(RatingBucketName.A, resA, null);

        Bucket bktF = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("JOHN"));
        Restriction resF = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), bktF);
        ratingRule.setRuleForBucket(RatingBucketName.F, null, resF);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setId(modelId);
        return ruleBasedModel;
    }

}
