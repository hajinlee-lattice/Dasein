package com.latticeengines.scoringapi.exposed.model.impl;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

import edu.emory.mathcs.backport.java.util.Collections;

public class DefaultModelJsonTypeHandlerUnitTestNG {

    private DefaultModelJsonTypeHandler defaultModelJsonTypeHandler = new DefaultModelJsonTypeHandler();

    @SuppressWarnings("unchecked")
    @Test(groups = "unit", enabled = true)
    public void testBucketPercileScore() {
        ScoringArtifacts scoringArtifacts = new ScoringArtifacts(null, null, null, null, null, null, null, null, null,
                generateDefaultBucketMetadataList());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 4), BucketName.A);
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 5), BucketName.A);
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 10), BucketName.B);
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 11), BucketName.B);
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 40), BucketName.C);
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 50), BucketName.C);
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 99), BucketName.C);
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 100), BucketName.C);

        scoringArtifacts = new ScoringArtifacts(null, null, null, null, null, null, null, null, null,
                Collections.emptyList());
        Assert.assertEquals(defaultModelJsonTypeHandler.bucketPercileScore(scoringArtifacts, 100), BucketName.A);

    }

    private List<BucketMetadata> generateDefaultBucketMetadataList() {
        List<BucketMetadata> bucketMetadataList = new ArrayList<BucketMetadata>();
        BucketMetadata bucket1 = new BucketMetadata();
        bucket1.setBucket(BucketName.A);
        bucket1.setLeftBoundScore(9);
        bucket1.setRightBoundScore(5);
        BucketMetadata bucket2 = new BucketMetadata();
        bucket2.setBucket(BucketName.B);
        bucket2.setLeftBoundScore(39);
        bucket2.setRightBoundScore(10);
        BucketMetadata bucket3 = new BucketMetadata();
        bucket3.setBucket(BucketName.C);
        bucket3.setLeftBoundScore(99);
        bucket3.setRightBoundScore(40);
        bucketMetadataList.add(bucket1);
        bucketMetadataList.add(bucket2);
        bucketMetadataList.add(bucket3);
        return bucketMetadataList;
    }
}
