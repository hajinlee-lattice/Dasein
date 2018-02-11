package com.latticeengines.scoring.util;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProbabilityScoreNormalizer implements ScoreNormalizer {

    private static final Logger log = LoggerFactory.getLogger(ProbabilityScoreNormalizer.class);

    private List<NormalizationBucket> normalizationBuckets;
    private final int defaultMinimumScorePercent = 5;
    private final int defaultMaximumScorePercent = 95;
    private double minimumScore;
    private double maximumScore;
    private double minimumProbability;
    private double maximumProbability;
    private boolean initialized;

    private List<Double> startProbabilities;
    private List<Double> endProbabilities;

    public ProbabilityScoreNormalizer(List<NormalizationBucket> buckets) {
        normalizationBuckets = buckets;
        minimumScore = defaultMinimumScorePercent;
        maximumScore = defaultMaximumScorePercent;
        initialize();
    }

    @Override
    public double normalize(double rawScore, InterpolationFunctionType interpFunction) {
        if (!isInitialized()) {
            return -1;
        }

        if (rawScore <= minimumProbability) {
            return minimumProbability == 0 ? 0
                    : interpolate(rawScore / minimumProbability, interpFunction) * minimumScore;
        }
        if (rawScore >= maximumProbability) {
            return maximumProbability == 1 ? 100
                    : interpolate((rawScore - maximumProbability) / (1.0 - maximumProbability), interpFunction)
                            * (100 - maximumScore) + maximumScore;
        }
        int bisectionIndex = NormalizationUtils.findBisectionIndex(rawScore, normalizationBuckets);
        double startPercentile;
        if (bisectionIndex > 0)
            startPercentile = normalizationBuckets.get(bisectionIndex - 1).getCumulativePercentage();
        else
            startPercentile = 0.0;

        double rawScorePercentile = normalizationBuckets.get(bisectionIndex).getCumulativePercentage()
                - startPercentile;
        double percentileToUse = (rawScore - startProbabilities.get(bisectionIndex))
                / (endProbabilities.get(bisectionIndex) - startProbabilities.get(bisectionIndex)) * rawScorePercentile
                + startPercentile;

        return NormalizationUtils.percentileScoreFunction(percentileToUse, minimumScore, maximumScore);
    }

    private void initialize() {
        if (normalizationBuckets == null || normalizationBuckets.size() == 0)
            throw new RuntimeException("No Probability NormalizationBuckets found in the Model");

        startProbabilities = normalizationBuckets.stream().map(NormalizationBucket::getStart)
                .collect(Collectors.toList());
        endProbabilities = normalizationBuckets.stream().map(NormalizationBucket::getEnd).collect(Collectors.toList());
        if (startProbabilities.stream().allMatch(s -> s == 0) || endProbabilities.stream().allMatch(e -> e == 0)) {
            log.error("Failed to Create Probability Score Normalizer, NormalizationBucket values are zeros");
            throw new RuntimeException(
                    "Failed to Create Probability Score Normalizer, NormalizationBucket values are zeros");
        }

        minimumProbability = startProbabilities.get(0);
        maximumProbability = endProbabilities.get(endProbabilities.size() - 1);
        if (startProbabilities.stream().allMatch(s -> s == minimumProbability)
                || endProbabilities.stream().allMatch(e -> e == maximumProbability)) {
            log.error("Failed to Create Probability Score Normalizer, NormalizationBucket values are all equal");
            throw new RuntimeException(
                    "Failed to Create Probability Score Normalizer, NormalizationBucket values all equal");
        }

        initialized = true;
    }

    private double interpolate(double score, InterpolationFunctionType interpFunction) {
        switch (interpFunction) {
        case Linear:
            return score;
        case NegativeConvex:
            return (Math.exp(-1 * score) - 1) / (Math.exp(-1) - 1);
        case PositiveConvex:
            return (Math.exp(score) - 1) / (Math.exp(1.0) - 1);
        default:
            return score;
        }
    }

    public double getMinimumScore() {
        return minimumScore;
    }

    public double getMaximumScore() {
        return maximumScore;
    }

    public double getMinimumProbability() {
        return minimumProbability;
    }

    public double getMaximumProbability() {
        return maximumProbability;
    }

    public boolean isInitialized() {
        return initialized;
    }

}
