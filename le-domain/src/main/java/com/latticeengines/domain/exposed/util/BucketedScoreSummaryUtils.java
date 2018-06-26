package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScore;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class BucketedScoreSummaryUtils {

    private static final String SCORE = "Score";
    private static final String TOTAL_EVENTS = "TotalEvents";
    private static final String TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";
    private static final int MIN_SCORE = 5;
    private static final int MAX_SCORE = 99;

    private static final Scheduler scheduler = Schedulers.newParallel("bucketed-score");

    public static BucketedScoreSummary generateBucketedScoreSummary(List<GenericRecord> pivotedRecords) {
        int cumulativeNumLeads = 0;
        double cumulativeNumConverted = 0;
        pivotedRecords.sort(Comparator.comparingDouble(g -> Double.valueOf(g.get(SCORE).toString())));
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
                        Double.valueOf(pivotedRecord.get(TOTAL_POSITIVE_EVENTS).toString()), cumulativeNumLeads,
                        cumulativeNumConverted);
                cumulativeNumLeads += new Long((long) pivotedRecord.get(TOTAL_EVENTS)).intValue();
                cumulativeNumConverted += (double) pivotedRecord.get(TOTAL_POSITIVE_EVENTS);
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

        double totalLift = cumulativeNumConverted / cumulativeNumLeads;

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
                bucketedScoreSummary.getBarLifts()[32 - i] = (totalLeadsConvertedInBar / totalLeadsInBar) / totalLift;
            }
        }
        return bucketedScoreSummary;
    }

    public static List<BucketMetadata> computeLift(BucketedScoreSummary scoreSummary,
                                                   List<BucketMetadata> bucketMetadataList) {
        sortBucketMetadata(bucketMetadataList);
        List<Integer> lowerBonds = null;
        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            if (lowerBonds == null) {
                lowerBonds = new ArrayList<>();
            } else {
                lowerBonds.add(bucketMetadata.getRightBoundScore());
            }
        }
        if (CollectionUtils.isEmpty(lowerBonds) || lowerBonds.size() + 1 != bucketMetadataList.size()) {
            throw new RuntimeException("Not enough lower bonds");
        }
        List<BucketedScore> boundaries = new ArrayList<>();
        boundaries.add( new BucketedScore(0, 0, 0, scoreSummary.getTotalNumLeads(), scoreSummary.getTotalNumConverted()));
        for (BucketedScore bucketedScore : scoreSummary.getBucketedScores()) {
            if (bucketedScore != null) {
                if (lowerBonds.contains(bucketedScore.getScore())) {
                    boundaries.add(bucketedScore);
                }
            }
        }
        boundaries.add( new BucketedScore(100, 0, 0, 0, 0));

        double overallConversion = scoreSummary.getTotalNumConverted() / scoreSummary.getTotalNumLeads();
        for (int i = 0; i < bucketMetadataList.size(); i++) {
            BucketedScore lowerBond = boundaries.get(i);
            BucketedScore upperBond = boundaries.get(i + 1);
            int totolLeads = lowerBond.getLeftNumLeads() + lowerBond.getNumLeads();
            totolLeads -= upperBond.getLeftNumLeads() + upperBond.getNumLeads();
            double lift;
            if (scoreSummary.getTotalNumConverted() == 0) {
                lift = 0.0D;
            } else {
                double totolConverted = lowerBond.getLeftNumConverted() + lowerBond.getNumConverted();
                totolConverted -= upperBond.getLeftNumConverted() + upperBond.getNumConverted();
                double conversionRate = totolLeads == 0 ? 0 : totolConverted / totolLeads;
                lift = conversionRate / overallConversion;
            }
            bucketMetadataList.get(i).setLift(lift);
            bucketMetadataList.get(i).setNumLeads(totolLeads);
        }

        return Lists.reverse(bucketMetadataList);
    }

    private static void sortBucketMetadata(List<BucketMetadata> bucketMetadata) {
        bucketMetadata.sort(Comparator.comparingInt(BucketMetadata::getRightBoundScore));
    }


}
