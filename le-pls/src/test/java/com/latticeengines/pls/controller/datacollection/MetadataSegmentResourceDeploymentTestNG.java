package com.latticeengines.pls.controller.datacollection;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
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
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
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
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private ActionProxy actionProxy;

    private String segmentName;
    private String ratingEngineId;
    private int actionNumber = 0;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        attachProtectedProxy(testSegmentProxy);
        attachProtectedProxy(testRatingEngineProxy);
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
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
        Assert.assertNotNull(returned.getName());
        Assert.assertEquals(returned.getName(), segmentName);
        segmentName = returned.getName();
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

    private Map<String, List<BucketMetadata>> generateRatingCounts(String ratingEngineId) {
        List<BucketMetadata> coverage = Arrays.asList(//
                new BucketMetadata(BucketName.A, 136), //
                new BucketMetadata(BucketName.D, 1018), //
                new BucketMetadata(BucketName.F, 88));
        return ImmutableMap.of(ratingEngineId, coverage);
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

        Assert.assertNotEquals(returned.getAccounts(), initialAccountNum, JsonUtils.serialize(returned));
        Assert.assertNotEquals(returned.getContacts(), initialContactNum, JsonUtils.serialize(returned));
        assertMetadataSegmentUpdateAction();
        RatingEngine ratingEngine = testRatingEngineProxy.getRatingEngine(ratingEngineId);
        Assert.assertNotNull(ratingEngine);

        Map<String, Long> ratingCounts = ratingEngine.getCountsAsMap();
        Assert.assertTrue(MapUtils.isNotEmpty(ratingCounts));
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdate")
    public void testSoftDelete() {
        try {
            segmentProxy.deleteSegmentByName(mainTestTenant.getId(), segmentName, true);
            Assert.fail("Should not be able to delete segment if rating engine is associated with it");
        } catch (Exception ex) {
            ratingEngineProxy.deleteRatingEngine(mainTestTenant.getId(), ratingEngineId, false,
                    "test@lattice-engines.com");
            segmentProxy.deleteSegmentByName(mainTestTenant.getId(), segmentName, false);
            MetadataSegment segment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), segmentName);
            List<String> deletedSegments = segmentProxy.getAllDeletedSegments(mainTestTenant.getId());
            Assert.assertEquals(deletedSegments.size(), 1);
            Assert.assertEquals(deletedSegments.get(0), segmentName);
            Assert.assertTrue(segment.getDeleted());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "testSoftDelete")
    public void testHardDelete() {
        segmentProxy.revertDeleteSegmentByName(mainTestTenant.getId(), segmentName);
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), segmentName);
        Assert.assertFalse(segment.getDeleted());
        segmentProxy.deleteSegmentByName(mainTestTenant.getId(), segmentName, true);
    }

    private List<Action> getSegmentActiosn() {
        String tenantId = MultiTenantContext.getShortTenantId();
        List<Action> actions = actionProxy.getActions(tenantId);
        return actions.stream() //
                .filter(action -> ActionType.METADATA_SEGMENT_CHANGE.equals(action.getType())) //
                .collect(Collectors.toList());
    }

    private void assertMetadataSegmentUpdateActionNotGen() {
        List<Action> actions = getSegmentActiosn();
        Assert.assertEquals(actions.size(), actionNumber);
    }

    private void assertMetadataSegmentUpdateAction() {
        List<Action> actions = getSegmentActiosn();
        Assert.assertEquals(actions.size(), ++actionNumber);
        Action action = actions.get(actionNumber - 1);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.METADATA_SEGMENT_CHANGE);
    }

    private RatingEngine createRuleBasedRatingEngine(MetadataSegment segment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(segment);
        ratingEngine.setCreatedBy("test@lattice-engines.com");
        ratingEngine.setUpdatedBy("test@lattice-engines.com");
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        RatingEngine retrievedRatingEngine = testRatingEngineProxy.createOrUpdate(ratingEngine);
        RatingEngine newEngine = testRatingEngineProxy.getRatingEngine(retrievedRatingEngine.getId());
        String modelId = newEngine.getLatestIteration().getId();
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
