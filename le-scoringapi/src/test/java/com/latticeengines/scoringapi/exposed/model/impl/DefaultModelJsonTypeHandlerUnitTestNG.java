package com.latticeengines.scoringapi.exposed.model.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreEvaluation;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

public class DefaultModelJsonTypeHandlerUnitTestNG {

    @Spy
    private DefaultModelJsonTypeHandler defaultModelJsonTypeHandler = new DefaultModelJsonTypeHandler();

    @SuppressWarnings("unchecked")
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        ScoreEvaluation scoreEvaluation = new ScoreEvaluation(0.05123, 99, BucketName.A_PLUS);
        doReturn(scoreEvaluation).when(defaultModelJsonTypeHandler).score(any(ScoringArtifacts.class), any(Map.class));
    }

    @Test(groups = "unit", enabled = true)
    public void testScoreResponseGetValue() {
        ScoringArtifacts scoringArtifacts = new ScoringArtifacts(null, null, null, null, null, null, null, null, null,
                generateDefaultBucketMetadataList());
        Map<String, Object> transformedRecord = new HashMap<String, Object>();
        ScoreResponse sr = defaultModelJsonTypeHandler.generateScoreResponse(scoringArtifacts, transformedRecord);
        Assert.assertEquals(sr.getBucket(), BucketName.A_PLUS.toValue());
    }

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

    @Test(groups = "unit", enabled = true)
    public void testHandleEmptyString() {
        Map<String, Object> copiedRecord = new HashMap<>();
        Map<String, Object> record = new HashMap<>();

        populateRecord(record);

        defaultModelJsonTypeHandler.handleEmptyString(record, copiedRecord);

        verifyCopiedMap(record, copiedRecord);
    }

    private void verifyCopiedMap(Map<String, Object> record, Map<String, Object> copiedRecord) {
        Assert.assertEquals(record.size(), 5);
        Assert.assertEquals(copiedRecord.size(), 5);

        for (String key : record.keySet()) {
            Object origValue = record.get(key);
            Assert.assertTrue(copiedRecord.containsKey(key));
            Object copiedValue = copiedRecord.get(key);

            if (origValue != null && origValue instanceof String) {
                CharSequence value = ((String) origValue);
                if (StringUtils.isBlank(value)) {
                    Assert.assertNull(copiedValue);
                } else {
                    Assert.assertEquals(copiedValue, origValue);
                }
            } else {
                Assert.assertEquals(copiedValue, origValue);
            }
        }
    }

    private void populateRecord(Map<String, Object> record) {
        record.put("K1", 2L);
        record.put("K2", "");
        record.put("K3", "   ");
        record.put("K4", null);
        record.put("K5", "Value");
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
