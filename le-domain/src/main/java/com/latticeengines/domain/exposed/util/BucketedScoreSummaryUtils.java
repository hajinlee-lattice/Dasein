package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
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
        pivotedRecords = combineNullAndMinScoreRecord(pivotedRecords);
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

    private static List<GenericRecord> combineNullAndMinScoreRecord(List<GenericRecord> pivotedRecords) {

        Double totalNullEvents = 0.;
        Double totalNullPositiveEvents = 0.;
        GenericRecord minScoreRecord = null;
        boolean hasNullScoreRecord = false;
        int currentMinScore = MAX_SCORE;

        for (GenericRecord record : pivotedRecords) {
            if (record.get(SCORE) == null) {
                Double nullEvents = record.get(TOTAL_EVENTS) == null ?
                        0. : Double.valueOf(record.get(TOTAL_EVENTS).toString());
                Double nullPositiveEvents = record.get(TOTAL_POSITIVE_EVENTS) == null ?
                        0. : Double.valueOf(record.get(TOTAL_POSITIVE_EVENTS).toString());
                totalNullEvents += nullEvents;
                totalNullPositiveEvents += nullPositiveEvents;
                hasNullScoreRecord = true;
            } else {
                int score = Double.valueOf(record.get(SCORE).toString()).intValue();
                if (score <= currentMinScore) {
                    minScoreRecord = record;
                    currentMinScore = score;
                }
            }
        }
        if (hasNullScoreRecord && minScoreRecord != null) {
            Double minTotalEvents = minScoreRecord.get(TOTAL_EVENTS) == null ?
                    0. : Double.valueOf(minScoreRecord.get(TOTAL_EVENTS).toString());
            Double minTotalPositiveEvents = minScoreRecord.get(TOTAL_POSITIVE_EVENTS) == null ?
                    0. : Double.valueOf(minScoreRecord.get(TOTAL_POSITIVE_EVENTS).toString());
            minTotalEvents += totalNullEvents;
            minTotalPositiveEvents += totalNullPositiveEvents;
            minScoreRecord.put(TOTAL_EVENTS, minTotalEvents.longValue());
            minScoreRecord.put(TOTAL_POSITIVE_EVENTS, minTotalPositiveEvents);
        }

        return pivotedRecords.stream().filter(record -> record.get(SCORE) != null).collect(Collectors.toList());
    }

    public static List<BucketMetadata> computeLift(BucketedScoreSummary scoreSummary,
                                                   List<BucketMetadata> bucketMetadataList) {
        bucketMetadataList = sortBucketMetadata(bucketMetadataList, true);
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
        boundaries.add(new BucketedScore(0, 0, 0, scoreSummary.getTotalNumLeads(), scoreSummary.getTotalNumConverted()));
        for (BucketedScore bucketedScore : scoreSummary.getBucketedScores()) {
            if (bucketedScore != null) {
                if (lowerBonds.contains(bucketedScore.getScore())) {
                    boundaries.add(bucketedScore);
                }
            }
        }
        boundaries.add(new BucketedScore(100, 0, 0, 0, 0));

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

    // sort bucket metadata by lower bond score asc
    public static List<BucketMetadata> sortBucketMetadata(List<BucketMetadata> bucketMetadata, boolean ascendingScore) {
        if (CollectionUtils.isNotEmpty(bucketMetadata)) {
            bucketMetadata.sort(Comparator.comparingInt(BucketMetadata::getRightBoundScore));
            if (!ascendingScore) {
                bucketMetadata = Lists.reverse(bucketMetadata);
            }
        }
        return bucketMetadata;
    }


}
