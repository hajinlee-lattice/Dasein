package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.BucketedScore;
import com.latticeengines.domain.exposed.pls.BucketedScoresSummary;
import com.wordnik.swagger.annotations.ApiOperation;

import io.swagger.annotations.Api;

@Api(value = "bucketedscores", description = "REST resource for bucketed scores")
@RestController
@RequestMapping("/bucketedscores")
// @PreAuthorize("View_PLS_ABCD_Bucketing")
public class BucketedScoresResource {

    private static final BucketedScoresSummary BUCKETED_SCORES_SUMMARY = new BucketedScoresSummary();
    private static final String SCORE = "Score";
    private static final String TOTAL_EVENTS = "TotalEvents";
    private static final String TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";

    @RequestMapping(value = "/summary/mocked/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get bucketed scores for specific model")
    public ResponseDocument<BucketedScoresSummary> getBuckedScoresSummary(
            @PathVariable String modelId) throws Exception {
        String path = Thread.currentThread().getContextClassLoader()
                .getResource("com/latticeengines/pls/controller/internal/part-00000.avro")
                .getPath();
        List<GenericRecord> pivotedRecords = AvroUtils.readFromLocalFile(path);

        int cumulativeNumLeads = 0, cumulativeNumConverted = 0;
        int currentRecord = pivotedRecords.size() - 1;

        for (int i = 99; i > 4; i--) {
            GenericRecord pivotedRecord = pivotedRecords.get(currentRecord);
            if ((int) pivotedRecord.get(SCORE) == i) {
                BUCKETED_SCORES_SUMMARY.getBucketedScores()[i] = new BucketedScore(
                        (int) pivotedRecord.get(SCORE),
                        new Long((long) pivotedRecord.get(TOTAL_EVENTS)).intValue(),
                        new Double((double) pivotedRecord.get(TOTAL_POSITIVE_EVENTS)).intValue(),
                        cumulativeNumLeads, cumulativeNumConverted);
                cumulativeNumLeads += new Long((long) pivotedRecord.get(TOTAL_EVENTS)).intValue();
                cumulativeNumConverted += new Double(
                        (double) pivotedRecord.get(TOTAL_POSITIVE_EVENTS)).intValue();
                currentRecord--;
            } else {
                BUCKETED_SCORES_SUMMARY.getBucketedScores()[i] = new BucketedScore(i, 0, 0,
                        cumulativeNumLeads, cumulativeNumConverted);
            }
        }
        BUCKETED_SCORES_SUMMARY.getBucketedScores()[4] = new BucketedScore(4, 0, 0,
                cumulativeNumLeads, cumulativeNumConverted);
        BUCKETED_SCORES_SUMMARY.setTotalNumLeads(cumulativeNumLeads);
        BUCKETED_SCORES_SUMMARY.setTotalNumConverted(cumulativeNumConverted);

        double totalLift = (double) cumulativeNumConverted / cumulativeNumLeads;
        for (int i = 32; i > 0; i--) {
            BucketedScore[] bucketedScores = BUCKETED_SCORES_SUMMARY.getBucketedScores();
            int totalLeadsInBar = bucketedScores[i * 3 + 1].getNumLeads()
                    + bucketedScores[i * 3 + 2].getNumLeads()
                    + bucketedScores[i * 3 + 3].getNumLeads();
            int totalLeadsConvertedInBar = bucketedScores[i * 3 + 1].getNumConverted()
                    + bucketedScores[i * 3 + 2].getNumConverted()
                    + bucketedScores[i * 3 + 3].getNumConverted();
            if (totalLeadsInBar == 0) {
                BUCKETED_SCORES_SUMMARY.getBarLifts()[32 - i] = 0;
            } else {
                BUCKETED_SCORES_SUMMARY.getBarLifts()[32
                        - i] = ((double) totalLeadsConvertedInBar / totalLeadsInBar) / totalLift;
            }
        }

        return ResponseDocument.successResponse(BUCKETED_SCORES_SUMMARY);
    }

    private static final BucketMetadata A_BUCKET_METADATA = new BucketMetadata();
    private static final BucketMetadata B_BUCKET_METADATA = new BucketMetadata();
    private static final BucketMetadata C_BUCKET_METADATA = new BucketMetadata();
    private static final BucketMetadata D_BUCKET_METADATA = new BucketMetadata();

    @RequestMapping(value = "/abcdbuckets/mocked/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets history info for the model")
    public ResponseDocument<List<BucketMetadata>> getABCDBuckets(@PathVariable String modelId) {
        A_BUCKET_METADATA.setBucketName(BucketName.A);
        A_BUCKET_METADATA.setLeftBoundScore(99);
        A_BUCKET_METADATA.setRightBoundScore(90);
        A_BUCKET_METADATA.setNumLeads(1623);
        A_BUCKET_METADATA.setLift(3.0);
        A_BUCKET_METADATA.setId("1");
        A_BUCKET_METADATA.setCreationTimestamp(1484559900000l);

        B_BUCKET_METADATA.setBucketName(BucketName.B);
        B_BUCKET_METADATA.setNumLeads(90);
        B_BUCKET_METADATA.setRightBoundScore(81);
        B_BUCKET_METADATA.setNumLeads(456);
        B_BUCKET_METADATA.setLift(2.3);
        B_BUCKET_METADATA.setId("1");
        B_BUCKET_METADATA.setCreationTimestamp(1484559900000l);

        C_BUCKET_METADATA.setBucketName(BucketName.C);
        C_BUCKET_METADATA.setLeftBoundScore(81);
        C_BUCKET_METADATA.setRightBoundScore(60);
        C_BUCKET_METADATA.setNumLeads(666);
        C_BUCKET_METADATA.setLift(1.3);
        C_BUCKET_METADATA.setId("1");
        C_BUCKET_METADATA.setCreationTimestamp(1484559900000l);

        D_BUCKET_METADATA.setBucketName(BucketName.D);
        D_BUCKET_METADATA.setLeftBoundScore(60);
        D_BUCKET_METADATA.setRightBoundScore(5);
        D_BUCKET_METADATA.setNumLeads(1416);
        D_BUCKET_METADATA.setLift(0.5);
        D_BUCKET_METADATA.setId("1");
        D_BUCKET_METADATA.setCreationTimestamp(1484559900000l);

        return ResponseDocument.successResponse(Arrays.asList(A_BUCKET_METADATA, B_BUCKET_METADATA,
                C_BUCKET_METADATA, D_BUCKET_METADATA));
    }

}
