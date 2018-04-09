package com.latticeengines.apps.lp.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.repository.reader.ModelSummaryReaderRepository;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.apps.lp.testframework.BucketedScoreTestUtils;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

public class BucketedScoreServiceImplTestNG extends LPFunctionalTestNGBase {

    private String modelGuid;
    private ModelSummary modelSummary;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Inject
    private ModelSummaryReaderRepository modelSummaryRepository;

    @Inject
    private BucketedScoreService bucketedScoreService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
        modelGuid = String.format("ms__%s__LETest", UUID.randomUUID().toString());
        modelSummary = BucketedScoreTestUtils.createModelSummary(modelGuid, mainTestTenant);
        modelSummaryEntityMgr.create(modelSummary);
    }


    @Test(groups = { "functional" })
    public void createGroupOfBucketMetadataForModel_assertCreated() throws Exception {
        long oldLastUpdateTime = modelSummary.getLastUpdateTime();

        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setBucketMetadataList(BucketedScoreTestUtils.getBucketMetadataList1());
        request.setModelGuid(modelGuid);
        bucketedScoreService.createABCDBucketsForModel(request);
        Thread.sleep(500);

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = bucketedScoreService
                .getModelBucketMetadataGroupedByCreationTimes(modelGuid);
        Long timestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0];
        modelSummary = modelSummaryRepository.findById(modelGuid);
        long newLastUpdateTime = modelSummary.getLastUpdateTime();
        assertTrue(newLastUpdateTime > oldLastUpdateTime);
        BucketedScoreTestUtils.testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(timestamp));
    }

    @Test(groups = { "functional" }, dependsOnMethods = "createGroupOfBucketMetadataForModel_assertCreated")
    public void createAnotherGroupsOfBucketMetadata_assertCreated() throws Exception {
        long oldLastUpdateTime = modelSummary.getLastUpdateTime();
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setBucketMetadataList(BucketedScoreTestUtils.getBucketMetadataList2());
        request.setModelGuid(modelGuid);
        bucketedScoreService.createABCDBucketsForModel(request);
        Thread.sleep(500);

        modelSummary = modelSummaryRepository.findById(modelGuid);
        long newLastUpdateTime = modelSummary.getLastUpdateTime();
        assertTrue(newLastUpdateTime > oldLastUpdateTime);

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = bucketedScoreService
                .getModelBucketMetadataGroupedByCreationTimes(modelGuid);
        assertEquals(creationTimeToBucketMetadatas.keySet().size(), 2);
        Long earlierTimestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0],
                laterTimestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[1];
        Long placeHolderTimestamp;
        if (earlierTimestamp > laterTimestamp) {
            placeHolderTimestamp = earlierTimestamp;
            earlierTimestamp = laterTimestamp;
            laterTimestamp = placeHolderTimestamp;
        }

        BucketedScoreTestUtils.testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(earlierTimestamp));
        BucketedScoreTestUtils.testSecondGroupBucketMetadata(creationTimeToBucketMetadatas.get(laterTimestamp));
    }

    @Test(groups = { "functional" }, dependsOnMethods = "createAnotherGroupsOfBucketMetadata_assertCreated")
    public void testGetUpToDateModelBucketMetadata() {
        List<BucketMetadata> bucketMetadatas = bucketedScoreService.getUpToDateModelBucketMetadata(modelGuid);
        BucketedScoreTestUtils.testSecondGroupBucketMetadata(bucketMetadatas);
    }

}
