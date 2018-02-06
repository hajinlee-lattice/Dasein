package com.latticeengines.scoring.util;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpectedRevenueScoreNormalizer implements ScoreNormalizer {

    private static final Logger log = LoggerFactory.getLogger(ExpectedRevenueScoreNormalizer.class);

    private List<NormalizationBucket> normalizationBuckets;
    private final int defaultMinimumScorePercent = 5;
    private final int defaultMaximumScorePercent = 95;
    private double minimumScore;
    private double maximumScore;
    private double minimumExpectedRevenue;
    private double maximumExpectedRevenue;

    private List<Double> startExpectedRevenues;
    private List<Double> endExpectedRevenues;

    public ExpectedRevenueScoreNormalizer(List<NormalizationBucket> buckets) {
        normalizationBuckets = buckets;
        minimumScore = defaultMinimumScorePercent;
        maximumScore = defaultMaximumScorePercent;
        initialize();
    }

    @Override
    public double normalize(double predictedRevenue, InterpolationFunctionType interpFunction) {
        if (predictedRevenue <= minimumExpectedRevenue) {
            return minimumExpectedRevenue == 0 ? 0
                    : minimumScore * Math.max(0, predictedRevenue) / minimumExpectedRevenue;
        }
        if (predictedRevenue >= maximumExpectedRevenue) {
            return 100 - (100 - minimumScore) * Math.exp(-1 * predictedRevenue / maximumExpectedRevenue);
        }

        int bisectionIndex = NormalizationUtils.findBisectionIndex(predictedRevenue, normalizationBuckets);
        double startPercentile;
        if (bisectionIndex > 0)
            startPercentile = normalizationBuckets.get(bisectionIndex - 1).getCumulativePercentage();
        else
            startPercentile = 0.0;
        double rawScorePercentile = normalizationBuckets.get(bisectionIndex).getCumulativePercentage()
                - startPercentile;
        double percentileToUse = (predictedRevenue - startExpectedRevenues.get(bisectionIndex))
                / (endExpectedRevenues.get(bisectionIndex) - startExpectedRevenues.get(bisectionIndex))
                * rawScorePercentile + startPercentile;
        return NormalizationUtils.percentileScoreFunction(percentileToUse, minimumScore, maximumScore);
    }

    private void initialize() {

        if (normalizationBuckets == null || normalizationBuckets.size() == 0)
            throw new RuntimeException("No ExpectedRevenue NormalizationBuckets found in the Model");
        startExpectedRevenues = normalizationBuckets.stream().map(NormalizationBucket::getStart)
                .collect(Collectors.toList());
        endExpectedRevenues = normalizationBuckets.stream().map(NormalizationBucket::getEnd)
                .collect(Collectors.toList());

        if (startExpectedRevenues.stream().allMatch(s -> s == 0)
                || endExpectedRevenues.stream().allMatch(e -> e == 0)) {
            log.error("Failed to Create ExpectedRevenue Score Normalizer, NormalizationBucket values are zeros");
            throw new RuntimeException(
                    "Failed to Create ExpectedRevenue Score Normalizer, NormalizationBucket values are zeros");
        }

        minimumExpectedRevenue = startExpectedRevenues.get(0);
        maximumExpectedRevenue = endExpectedRevenues.get(endExpectedRevenues.size() - 1);
        if (startExpectedRevenues.stream().allMatch(s -> s == minimumExpectedRevenue)
                || endExpectedRevenues.stream().allMatch(e -> e == maximumExpectedRevenue)) {
            log.error("Failed to Create ExpectedRevenue Score Normalizer, NormalizationBucket values are all equal");
            throw new RuntimeException(
                    "Failed to Create ExpectedRevenue Score Normalizer, NormalizationBucket values all equal");

        }

    }

}
