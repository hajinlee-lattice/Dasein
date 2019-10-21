package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.Serializable;

import com.latticeengines.domain.exposed.scoringapi.BucketRange;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class RawScoreToPercentileMapper implements Serializable {
    private static final long serialVersionUID = -7382907476491844959L;
    private static final int MIN_PERCENTILE = 5;
    private static final int MAX_PERCENTILE = 99;

    private ScoreDerivation derivation;

    public RawScoreToPercentileMapper(ScoreDerivation scoreDerivation) {
        this.derivation = checkDerivation(scoreDerivation);
    }

    private ScoreDerivation checkDerivation(ScoreDerivation scoreDerivation) {
        if (scoreDerivation.percentiles == null) {
            throw new IllegalArgumentException("No percentile buckets found");
        } else if (scoreDerivation.percentiles.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "No buckets in scoreDerivation. size:%d", scoreDerivation.percentiles.size()));
        }
        return scoreDerivation;
    }

    protected double getLowerBoundValue() {
        return 0.0;
    }

    protected double getUpperBoundValue() {
        return 1.0;
    }

    public int map(double rawScore) {

        double lowest = getUpperBoundValue();
        double highest = getLowerBoundValue();
        Integer percentile = null;

        for (int index = 0; index < derivation.percentiles.size(); index++) {
            BucketRange percentileRange = derivation.percentiles.get(index);
            if (percentileRange.lower < lowest) {
                lowest = percentileRange.lower;
            } else if (percentileRange.upper > highest) {
                highest = percentileRange.upper;
            }
            if (withinRange(percentileRange, rawScore)) {
                // Name of the percentile bucket is the percentile value.
                percentile = Integer.valueOf(percentileRange.name);
            }
        }
        // value out of range
        if (percentile == null) {
            if (rawScore <= lowest) {
                percentile = MIN_PERCENTILE;
            } else {
                percentile = MAX_PERCENTILE;
            }
        }

        return percentile;
    }

    private boolean withinRange(BucketRange range, //
            double value) {
        return (range.lower == null || value >= range.lower)
                && (range.upper == null || value <= range.upper);
    }
}
