package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScore;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;

public class BucketedScoreSummaryUtils {
    private static final Logger log = LoggerFactory.getLogger(BucketedScoreSummaryUtils.class);

    public static final String TOTAL_EVENTS = "TotalEvents";
    public static final String TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";
    public static final int MIN_SCORE = 5;
    public static final int MAX_SCORE = 99;

    public static final String BUCKET_AVG_SCORE = "BucketAverageScore";
    public static final String BUCKET_SUM = "BucketSum";

    public static final String BUCKET_TOTAL_EVENTS = "TotalEvents";
    public static final String BUCKET_TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";
    public static final String BUCKET_LIFT = "Lift";
    public static final String MODEL_GUID = ScoreResultField.ModelId.displayName;

    public static final String MODEL_AVG = "ModelAvg";
    public static final String MODEL_SUM = "ModelSum";

    public static BucketedScoreSummary generateBucketedScoreSummary(List<GenericRecord> pivotedRecords) {
        return generateBucketedScoreSummary(pivotedRecords, false);
    }

    public static BucketedScoreSummary generateBucketedScoreSummary(List<GenericRecord> pivotedRecords, boolean isEV) {
        int cumulativeNumLeads = 0;
        double cumulativeNumConverted = 0;
        double totalExpectedRevenue = 0;
        pivotedRecords = combineNullAndMinScoreRecord(pivotedRecords, isEV);
        pivotedRecords.sort(Comparator.comparingDouble(g -> Double.valueOf(g.get(scoreColumn(isEV)).toString())));
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
                    && Double.valueOf(pivotedRecord.get(scoreColumn(isEV)).toString()).intValue() == currentScore) {
                bucketedScoreSummary.getBucketedScores()[currentScore] = new BucketedScore(
                        Double.valueOf(pivotedRecord.get(scoreColumn(isEV)).toString()).intValue(),
                        Double.valueOf(pivotedRecord.get(TOTAL_EVENTS).toString()).intValue(),
                        Double.valueOf(pivotedRecord.get(TOTAL_POSITIVE_EVENTS).toString()), cumulativeNumLeads,
                        cumulativeNumConverted,
                        isEV ? Double.valueOf(pivotedRecord.get(BUCKET_AVG_SCORE).toString()).doubleValue() : null,
                        isEV ? Double.valueOf(pivotedRecord.get(BUCKET_SUM).toString()).doubleValue() : null,
                        isEV ? totalExpectedRevenue : null);
                cumulativeNumLeads += new Long((long) pivotedRecord.get(TOTAL_EVENTS)).intValue();
                cumulativeNumConverted += (double) pivotedRecord.get(TOTAL_POSITIVE_EVENTS);
                if (isEV) {
                    totalExpectedRevenue += Double.valueOf(pivotedRecord.get(BUCKET_SUM).toString()).doubleValue();
                }
                idx--;
            } else {
                bucketedScores[currentScore] = new BucketedScore(currentScore, 0, 0D, cumulativeNumLeads,
                        cumulativeNumConverted, //
                        isEV ? 0D : null, //
                        isEV ? 0D : null, //
                        isEV ? totalExpectedRevenue : null);
            }
            currentScore--;
        }
        for (; currentScore > 3; currentScore--) {
            bucketedScores[currentScore] = new BucketedScore(currentScore, 0, 0D, cumulativeNumLeads,
                    cumulativeNumConverted, //
                    isEV ? 0D : null, //
                    isEV ? 0D : null, //
                    isEV ? totalExpectedRevenue : null);
        }

        double totalLift = cumulativeNumConverted / cumulativeNumLeads;
        if (isEV) {
            totalLift = totalExpectedRevenue / cumulativeNumLeads;
        }

        bucketedScoreSummary.setTotalNumLeads(cumulativeNumLeads);
        bucketedScoreSummary.setTotalNumConverted(cumulativeNumConverted);
        bucketedScoreSummary.setTotalExpectedRevenue(isEV ? totalExpectedRevenue : null);
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
                if (isEV) {
                    bucketedScoreSummary.getBarLifts()[32 - i] = ((bucketedScores[i * 3 + 1].getExpectedRevenue()
                            + bucketedScores[i * 3 + 2].getExpectedRevenue()
                            + bucketedScores[i * 3 + 3].getExpectedRevenue()) / totalLeadsInBar) / totalLift;
                }
            }
        }
        return bucketedScoreSummary;
    }

    private static List<GenericRecord> combineNullAndMinScoreRecord(List<GenericRecord> pivotedRecords, boolean isEV) {

        Double totalNullEvents = 0.;
        Double totalNullPositiveEvents = 0.;
        GenericRecord minScoreRecord = null;
        boolean hasNullScoreRecord = false;
        int currentMinScore = MAX_SCORE;

        for (GenericRecord record : pivotedRecords) {
            if (record.get(scoreColumn(isEV)) == null) {
                Double nullEvents = record.get(TOTAL_EVENTS) == null ? 0.
                        : Double.valueOf(record.get(TOTAL_EVENTS).toString());
                Double nullPositiveEvents = record.get(TOTAL_POSITIVE_EVENTS) == null ? 0.
                        : Double.valueOf(record.get(TOTAL_POSITIVE_EVENTS).toString());
                totalNullEvents += nullEvents;
                totalNullPositiveEvents += nullPositiveEvents;
                hasNullScoreRecord = true;
            } else {
                int score = Double.valueOf(record.get(scoreColumn(isEV)).toString()).intValue();
                if (score <= currentMinScore) {
                    minScoreRecord = record;
                    currentMinScore = score;
                }
            }
        }
        if (hasNullScoreRecord && minScoreRecord != null) {
            Double minTotalEvents = minScoreRecord.get(TOTAL_EVENTS) == null ? 0.
                    : Double.valueOf(minScoreRecord.get(TOTAL_EVENTS).toString());
            Double minTotalPositiveEvents = minScoreRecord.get(TOTAL_POSITIVE_EVENTS) == null ? 0.
                    : Double.valueOf(minScoreRecord.get(TOTAL_POSITIVE_EVENTS).toString());
            minTotalEvents += totalNullEvents;
            minTotalPositiveEvents += totalNullPositiveEvents;
            minScoreRecord.put(TOTAL_EVENTS, minTotalEvents.longValue());
            minScoreRecord.put(TOTAL_POSITIVE_EVENTS, minTotalPositiveEvents);
        }

        return pivotedRecords.stream().filter(record -> record.get(scoreColumn(isEV)) != null)
                .collect(Collectors.toList());
    }

    public static List<BucketMetadata> computeLift(BucketedScoreSummary scoreSummary,
            List<BucketMetadata> bucketMetadataList, boolean isEV) {
        log.info(String.format("isEV = %s", isEV));
        bucketMetadataList = sortBucketMetadata(bucketMetadataList, true);
        List<Integer> lowerBounds = null;
        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            if (lowerBounds == null) {
                lowerBounds = new ArrayList<>();
            } else {
                lowerBounds.add(bucketMetadata.getRightBoundScore());
            }
        }
        if (CollectionUtils.isEmpty(lowerBounds) || lowerBounds.size() + 1 != bucketMetadataList.size()) {
            throw new RuntimeException("Not enough lower bounds");
        }
        List<BucketedScore> boundaries = new ArrayList<>();
        boundaries.add(new BucketedScore(0, 0, 0D, //
                scoreSummary.getTotalNumLeads(), scoreSummary.getTotalNumConverted(), isEV ? 0D : null, //
                isEV ? 0D : null, //
                isEV ? scoreSummary.getTotalExpectedRevenue() : null));
        for (BucketedScore bucketedScore : scoreSummary.getBucketedScores()) {
            if (bucketedScore != null) {
                if (lowerBounds.contains(bucketedScore.getScore())) {
                    boundaries.add(bucketedScore);
                }
            }
        }
        boundaries.add(new BucketedScore(100, 0, 0D, 0, 0D, //
                isEV ? 0D : null, //
                isEV ? 0D : null, //
                isEV ? 0D : null));

        int totalNumLeads = scoreSummary.getTotalNumLeads();
        double overallConversion = scoreSummary.getTotalNumConverted() / totalNumLeads;
        double totalExpectedRevenue = 0;
        double overallAverageExpectedRevenue = 0;

        for (int i = 0; i < bucketMetadataList.size(); i++) {
            BucketedScore lowerBound = boundaries.get(i);
            BucketedScore upperBound = boundaries.get(i + 1);
            int totolLeads = lowerBound.getLeftNumLeads() + lowerBound.getNumLeads();
            totolLeads -= (upperBound.getLeftNumLeads() + upperBound.getNumLeads());
            double lift;
            if (scoreSummary.getTotalNumConverted() == 0) {
                lift = 0.0D;
            } else {
                double totolConverted = lowerBound.getLeftNumConverted() + lowerBound.getNumConverted();
                totolConverted -= upperBound.getLeftNumConverted() + upperBound.getNumConverted();
                double conversionRate = totolLeads == 0 ? 0 : totolConverted / totolLeads;
                lift = conversionRate / overallConversion;
            }
            bucketMetadataList.get(i).setLift(lift);
            bucketMetadataList.get(i).setNumLeads(totolLeads);
            if (isEV) {
                if (totalExpectedRevenue == 0) {
                    totalExpectedRevenue = scoreSummary.getTotalExpectedRevenue();
                    overallAverageExpectedRevenue = totalExpectedRevenue / totalNumLeads;
                }

                double totalBucketExpectedRevenue = lowerBound.getLeftExpectedRevenue()
                        + lowerBound.getExpectedRevenue();
                totalBucketExpectedRevenue -= upperBound.getLeftExpectedRevenue() + upperBound.getExpectedRevenue();
                double averageBucketExpectedRevenue = totolLeads == 0 ? 0 : totalBucketExpectedRevenue / totolLeads;
                bucketMetadataList.get(i).setTotalExpectedRevenue(totalBucketExpectedRevenue);
                bucketMetadataList.get(i).setAverageExpectedRevenue(averageBucketExpectedRevenue);
                if (overallAverageExpectedRevenue > 0) {
                    lift = averageBucketExpectedRevenue / overallAverageExpectedRevenue;
                    bucketMetadataList.get(i).setLift(lift);
                }
            }
        }

        return Lists.reverse(bucketMetadataList);
    }

    // sort bucket metadata by lower bound score asc
    public static List<BucketMetadata> sortBucketMetadata(List<BucketMetadata> bucketMetadata, boolean ascendingScore) {
        if (CollectionUtils.isNotEmpty(bucketMetadata)) {
            bucketMetadata.sort(Comparator.comparingInt(BucketMetadata::getRightBoundScore));
            if (!ascendingScore) {
                bucketMetadata = Lists.reverse(bucketMetadata);
            }
        }
        return bucketMetadata;
    }

    private static String scoreColumn(boolean isEV) {
        return ScoreResultField.Percentile.displayName;
    }
}
