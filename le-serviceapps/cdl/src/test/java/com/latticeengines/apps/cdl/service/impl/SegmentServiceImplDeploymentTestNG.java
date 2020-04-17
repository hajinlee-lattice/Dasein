package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.cdl.UpdateSegmentCountResponse;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

/**
 * dpltc deploy -a admin,pls,lp,cdl,objectapi,matchapi,metadata
 */
public class SegmentServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private SegmentService segmentService;

    private String segmentName1;
    private String segmentName2;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId(), 4);
        createSegments();
    }

    @Test(groups = "deployment-app")
    public void testUpdateCounts() {
        MetadataSegment segment1 = segmentService.findByName(segmentName1);
        Date updated1 = segment1.getUpdated();
        MetadataSegment segment2 = segmentService.findByName(segmentName2);
        Date updated2 = segment2.getUpdated();

        SleepUtils.sleep(2000L);
        UpdateSegmentCountResponse response = segmentService.updateSegmentsCounts();
        Map<String, Map<BusinessEntity, Long>> review = response.getUpdatedCounts();
        Assert.assertEquals(review.size(), 3);
        Assert.assertTrue(review.containsKey(segmentName1));
        Assert.assertTrue(review.containsKey(segmentName2));
        review.values().forEach(counts -> Assert.assertEquals(CollectionUtils.size(counts), 2));

        segment1 = segmentService.findByName(segmentName1);
        Assert.assertEquals(segment1.getUpdated(), updated1);
        segment2 = segmentService.findByName(segmentName2);
        Assert.assertEquals(segment2.getUpdated(), updated2);
    }

    private void createSegments() {
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName("Segment 1");
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.IN_COLLECTION,
                Arrays.asList("CALIFORNIA", "TEXAS", "MICHIGAN", "NEW YORK"));
        BucketRestriction stateRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "State"),
                stateBkt);
        Restriction accountRestriction = Restriction.builder().or(stateRestriction).build();
        segment.setAccountRestriction(accountRestriction);
        MetadataSegment created = segmentService.createOrUpdateSegment(segment);
        segmentName1 = created.getName();

        segment = new MetadataSegment();
        segment.setDisplayName("Segment 2");
        Bucket techBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("General Practice"));
        BucketRestriction techRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "SpendAnalyticsSegment"), techBkt);
        accountRestriction = Restriction.builder().and(techRestriction).build();
        Bucket titleBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        BucketRestriction titleRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        Restriction contactRestriction = Restriction.builder().and(titleRestriction).build();
        segment.setAccountRestriction(accountRestriction);
        segment.setAccountRestriction(contactRestriction);
        created = segmentService.createOrUpdateSegment(segment);
        segmentName2 = created.getName();
    }

}
