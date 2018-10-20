package com.latticeengines.apps.lp.entitymgr.impl;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.entitymgr.BucketMetadataEntityMgr;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.testframework.BucketedScoreTestUtils;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public class BucketMetadataEntityMgrImplTestNG extends LPFunctionalTestNGBase {

    private String modelGuid;
    private ModelSummary modelSummary;

    @Inject
    private BucketMetadataEntityMgr entityMgr;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
        modelGuid = String.format("ms__%s__LETest", UUID.randomUUID().toString());
        modelSummary = BucketedScoreTestUtils.createModelSummary(modelGuid, mainTestTenant);
        modelSummaryEntityMgr.create(modelSummary);
    }

    @Test(groups = "functional")
    public void testGetByModelGuid() {
        List<BucketMetadata> metadataList = entityMgr
                .getBucketMetadatasForModelFromReader(modelGuid);
        Assert.assertTrue(CollectionUtils.isEmpty(metadataList));

        List<BucketMetadata> toSave = BucketedScoreTestUtils.getBucketMetadataList1();
        toSave.forEach(bucketMetadata -> bucketMetadata.setModelSummary(modelSummary));
        entityMgr.createBucketMetadata(toSave, modelGuid, null);

        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);

        retry.execute(context -> {
            List<BucketMetadata> metadataList1 = entityMgr
                    .getBucketMetadatasForModelFromReader(modelGuid);
            Assert.assertFalse(CollectionUtils.isEmpty(metadataList1));
            Assert.assertEquals(metadataList1.size(),
                    BucketedScoreTestUtils.getBucketMetadataList1().size());
            return metadataList1;
        });

        retry.execute(context -> {
            List<BucketMetadata> metadataList1 = entityMgr.getUpToDateBucketMetadatasForModelFromReader(modelGuid);
            Assert.assertNotNull(metadataList1);
            Assert.assertEquals(metadataList1.size(),
                    BucketedScoreTestUtils.getBucketMetadataList1().size());
            return metadataList1;
        });

    }

}
