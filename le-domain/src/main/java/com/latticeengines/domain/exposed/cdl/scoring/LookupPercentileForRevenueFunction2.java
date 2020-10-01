package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.scoringapi.BucketRange;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class LookupPercentileForRevenueFunction2 implements Serializable {

    private static final long serialVersionUID = -363852169874456400L;

    private List<BucketRange> percentiles;

    public LookupPercentileForRevenueFunction2(ScoreDerivation revenueScoreDerivation) {
        this.percentiles = revenueScoreDerivation.percentiles;

    }

    public Integer calculate(Double revenue) {
        String percentileStr = percentiles.stream() //
                .parallel() //
                .filter(p -> p.lower <= revenue && revenue < p.upper) //
                .map(p -> {
                    return p.name;
                }) //
                .findFirst().orElse(null);

        if (StringUtils.isBlank(percentileStr)) {
            if (percentiles.get(percentiles.size() - 1).upper <= revenue) {
                percentileStr = percentiles.get(percentiles.size() - 1).name;
            } else {
                throw new RuntimeException("Could not fine the percentile!");
            }
        }

        percentileStr = percentileStr.trim();
        Integer revenuePercentileBasedOnScoreDerivation = Integer.parseInt(percentileStr);
        return revenuePercentileBasedOnScoreDerivation;
    }
}
