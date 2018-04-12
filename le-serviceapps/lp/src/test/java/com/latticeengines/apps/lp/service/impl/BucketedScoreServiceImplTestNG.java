package com.latticeengines.apps.lp.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.repository.reader.ModelSummaryReaderRepository;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.apps.lp.testframework.BucketedScoreTestUtils;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;

public class BucketedScoreServiceImplTestNG extends LPFunctionalTestNGBase {

    private String modelGuid;
    private ModelSummary modelSummary;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Inject
    private ModelSummaryReaderRepository modelSummaryReaderRepository;

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
        bucketedScoreService.createABCDBuckets(request);
        Thread.sleep(500);

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = bucketedScoreService
                .getModelBucketMetadataGroupedByCreationTimes(modelGuid);
        Long timestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0];
        modelSummary = modelSummaryReaderRepository.findById(modelGuid);
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
        bucketedScoreService.createABCDBuckets(request);
        Thread.sleep(500);

        modelSummary = modelSummaryReaderRepository.findById(modelGuid);
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
        List<BucketMetadata> bucketMetadatas = bucketedScoreService.getABCDBucketsByModelGuid(modelGuid);
        BucketedScoreTestUtils.testSecondGroupBucketMetadata(bucketMetadatas);
    }

    @Test(groups = { "functional" })
    public void createBucketedScoreSummary() throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream("bucketedscoresummary/part-00000.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(is);
        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils.generateBucketedScoreSummary(records);
        bucketedScoreService.createOrUpdateBucketedScoreSummary(modelSummary.getId(), bucketedScoreSummary);
        Thread.sleep(500);
        BucketedScoreSummary retrieved = bucketedScoreService.getBucketedScoreSummaryByModelGuid(modelSummary.getId());
        assertEquals(Math.round(retrieved.getTotalNumConverted()), 906);

        bucketedScoreSummary = BucketedScoreSummaryUtils.generateBucketedScoreSummary(records);
        bucketedScoreSummary.setTotalNumConverted(bucketedScoreSummary.getTotalNumConverted() + 2.0);
        bucketedScoreService.createOrUpdateBucketedScoreSummary(modelSummary.getId(), bucketedScoreSummary);
        Thread.sleep(500);
        retrieved = bucketedScoreService.getBucketedScoreSummaryByModelGuid(modelSummary.getId());
        assertEquals(Math.round(retrieved.getTotalNumConverted()), 908);

        logger.info(bucketedScoreSummary.getBucketedScores()[4]);
        logger.info(retrieved.getBucketedScores()[4]);
        assertEquals(bucketedScoreSummary.getTotalNumConverted(), retrieved.getTotalNumConverted());
        assertEquals(bucketedScoreSummary.getOverallLift(), retrieved.getOverallLift());
        assertEquals(bucketedScoreSummary.getTotalNumLeads(), retrieved.getTotalNumLeads());
        assertNotNull(retrieved.getBarLifts());
        assertNotNull(retrieved.getBucketedScores());
    }

}
