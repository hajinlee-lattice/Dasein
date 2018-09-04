package com.latticeengines.dataflow.runtime.cascading.cdl;

import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class RevenueToPercentileMapper extends RawScoreToPercentileMapper {
    public RevenueToPercentileMapper(ScoreDerivation scoreDerivation) {
        super(scoreDerivation);
    }

    @Override
    protected double getLowerBoundValue() {
        return Double.MIN_VALUE;
    }

    @Override
    protected double getUpperBoundValue() {
        return Double.MAX_VALUE;
    }
}
