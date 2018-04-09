package com.latticeengines.domain.exposed.util;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.latticeengines.domain.exposed.pls.BucketedScore;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

public class BucketedScoreSummaryUtils {

    private static final String SCORE = "Score";
    private static final String TOTAL_EVENTS = "TotalEvents";
    private static final String TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";
    private static final int MIN_SCORE = 5;
    private static final int MAX_SCORE = 99;

    public static BucketedScoreSummary generateBucketedScoreSummary(List<GenericRecord> pivotedRecords) {
        int cumulativeNumLeads = 0;
        double cumulativeNumConverted = 0;

        int idx = pivotedRecords.size() - 1;
        int currentScore = MAX_SCORE;

        BucketedScoreSummary bucketedScoreSummary = new BucketedScoreSummary();
        BucketedScore[] bucketedScores = bucketedScoreSummary.getBucketedScores();
        while (currentScore >= MIN_SCORE) {
            if (idx < 0) {
                break;
            }
            GenericRecord pivotedRecord = pivotedRecords.get(idx);

            if (pivotedRecord != null
                    && Double.valueOf(pivotedRecord.get(SCORE).toString()).intValue() == currentScore) {
                bucketedScoreSummary.getBucketedScores()[currentScore] = new BucketedScore(
                        Double.valueOf(pivotedRecord.get(SCORE).toString()).intValue(),
                        Double.valueOf(pivotedRecord.get(TOTAL_EVENTS).toString()).intValue(),
                        Double.valueOf(pivotedRecord.get(TOTAL_POSITIVE_EVENTS).toString()).doubleValue(),
                        cumulativeNumLeads, cumulativeNumConverted);
                cumulativeNumLeads += new Long((long) pivotedRecord.get(TOTAL_EVENTS)).intValue();
                cumulativeNumConverted += new Double((double) pivotedRecord.get(TOTAL_POSITIVE_EVENTS)).doubleValue();
                idx--;
            } else {
                bucketedScores[currentScore] = new BucketedScore(currentScore, 0, 0, cumulativeNumLeads,
                        cumulativeNumConverted);
            }
            currentScore--;
        }
        for (; currentScore > 3; currentScore--) {
            bucketedScores[currentScore] = new BucketedScore(currentScore, 0, 0, cumulativeNumLeads,
                    cumulativeNumConverted);
        }

        double totalLift = (double) cumulativeNumConverted / cumulativeNumLeads;

        bucketedScoreSummary.setTotalNumLeads(cumulativeNumLeads);
        bucketedScoreSummary.setTotalNumConverted(cumulativeNumConverted);
        bucketedScoreSummary.setOverallLift(totalLift);

        for (int i = bucketedScoreSummary.getBarLifts().length; i > 0; i--) {
            int totalLeadsInBar = bucketedScores[i * 3 + 1].getNumLeads() + bucketedScores[i * 3 + 2].getNumLeads()
                    + bucketedScores[i * 3 + 3].getNumLeads();
            double totalLeadsConvertedInBar = bucketedScores[i * 3 + 1].getNumConverted()
                    + bucketedScores[i * 3 + 2].getNumConverted() + bucketedScores[i * 3 + 3].getNumConverted();
            if (totalLeadsInBar == 0) {
                bucketedScoreSummary.getBarLifts()[32 - i] = 0;
            } else {
                bucketedScoreSummary.getBarLifts()[32 - i] = ((double) totalLeadsConvertedInBar / totalLeadsInBar)
                        / totalLift;
            }
        }
        return bucketedScoreSummary;
    }
}
