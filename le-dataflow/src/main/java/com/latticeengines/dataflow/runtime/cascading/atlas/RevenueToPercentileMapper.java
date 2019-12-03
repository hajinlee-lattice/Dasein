package com.latticeengines.dataflow.runtime.cascading.cdl;

import com.latticeengines.domain.exposed.cdl.scoring.RawScoreToPercentileMapper;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class RevenueToPercentileMapper extends RawScoreToPercentileMapper {
    private static final long serialVersionUID = 3934385097877681198L;

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
