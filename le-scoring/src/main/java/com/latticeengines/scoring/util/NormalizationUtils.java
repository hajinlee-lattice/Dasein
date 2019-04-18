package com.latticeengines.scoring.util;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class NormalizationUtils {

    private static final double offset = 1.1; // Magic Number given by the
                                              // DataScience Team

    public static double percentileScoreFunction(double percentile, double minimumScore, double maximumScore) {
        double finalScore = minimumScore + (maximumScore - minimumScore) / 100.0 * scoreFunction(percentile);
        if (finalScore > 100)
            return 100;
        else if (finalScore < 0)
            return 0;
        else
            return finalScore;
    }

    private static double scoreFunction(double percentile) {
        return (100.0 * Math.log(1.0 - percentile / offset) / Math.log(1.0 - 1.0 / offset));
    }

    public static int findBisectionIndex(double predictedRevenue, List<NormalizationBucket> normalizationBuckets) {
        for (int i = 0; i < normalizationBuckets.size(); i++) {
            NormalizationBucket bucket = normalizationBuckets.get(i);
            if (predictedRevenue >= bucket.getStart() && predictedRevenue < bucket.getEnd()) {
                return i;
            }
        }
        return 0;
    }

    public static ScoreNormalizer getScoreNormalizer(boolean hasRevenue, boolean cdl, JsonNode model) {
        ScoreNormalizer normalizer = null;
        if (cdl) {
            if (hasRevenue) {
                List<NormalizationBucket> buckets = getNormalizationBuckets(model,
                        ScoringDaemonService.NORMALIZATION_EXPECTEDREVENUE);
                normalizer = new ExpectedRevenueScoreNormalizer(buckets);
            } else {
                List<NormalizationBucket> buckets = getNormalizationBuckets(model,
                        ScoringDaemonService.NORMALIZATION_PROBABILITY);
                normalizer = new ProbabilityScoreNormalizer(buckets);
            }
        }
        return normalizer;
    }

    private static List<NormalizationBucket> getNormalizationBuckets(JsonNode model, String bucketType) {
        List<NormalizationBucket> result = new ArrayList<>();
        JsonNode buckets = model.get(ScoringDaemonService.NORMALIZATION_BUCKETS);
        ArrayNode normalizationBuckets = (ArrayNode) buckets.get(bucketType);
        if (normalizationBuckets != null) {
            for (int i = 0; i < normalizationBuckets.size(); i++) {
                JsonNode normalizationBucket = normalizationBuckets.get(i);
                JsonNode startNode = normalizationBucket.get(ScoringDaemonService.NORMALIZATION_START);
                JsonNode endNode = normalizationBucket.get(ScoringDaemonService.NORMALIZATION_END);
                JsonNode cumPctNode = normalizationBucket.get(ScoringDaemonService.NORMALIZATION_CUMULATIVEPERCENTAGE);
                Double start = startNode.isNull() ? 0 : startNode.asDouble();
                Double end = endNode.isNull() ? 0 : endNode.asDouble();
                Double cumPct = cumPctNode.isNull() ? 0 : cumPctNode.asDouble();
                result.add(new NormalizationBucket(start, end, cumPct));
            }
        } else {
            throw new RuntimeException("There's no normalization buckets in the model!");
        }
        return result;
    }
}
