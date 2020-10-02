package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.BucketRange;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class CalculatePercentile2 implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(CalculatePercentile2.class);

    private static final long serialVersionUID = 7182390777059974450L;
    private boolean targetScoreDerivation;
    private List<BucketRange> existingPercentiles;
    private SimplePercentileCalculator percentileCalculator;

    public CalculatePercentile2(int minPct, int maxPct, boolean targetScoreDerivation,
            String targetScoreDerivationStr) {
        this.targetScoreDerivation = targetScoreDerivation;
        percentileCalculator = new SimplePercentileCalculator(minPct, maxPct);
        if (targetScoreDerivation && targetScoreDerivationStr != null) {
            ScoreDerivation scoreDerivation = JsonUtils.deserialize(targetScoreDerivationStr, ScoreDerivation.class);
            if (scoreDerivation != null) {
                this.existingPercentiles = scoreDerivation.percentiles;
            }
        }
    }

    public Integer calculate(Double rawScore, long totalCount, long currentPos) {
        if (targetScoreDerivation && existingPercentiles != null) {
            Integer pct = lookup(rawScore);
            return pct;
        }

        Integer pct = percentileCalculator.compute(totalCount, currentPos, rawScore);
        return pct;
    }

    private Integer lookup(Double score) {
        String percentileStr = existingPercentiles.stream() //
                .parallel().filter(p -> {
                    return p.lower <= score && score < p.upper;
                }).map(p -> {
                    return p.name;
                }).findFirst().orElse(null);

        if (StringUtils.isBlank(percentileStr)) {
            if (existingPercentiles.get(0).lower >= score) {
                percentileStr = existingPercentiles.get(0).name;
            } else if (existingPercentiles.get(existingPercentiles.size() - 1).upper <= score) {
                percentileStr = existingPercentiles.get(existingPercentiles.size() - 1).name;
            } else {
                throw new RuntimeException(String.format("Score %d is out of bound.", score));
            }
        }
        Integer lookupPercentile = Integer.parseInt(percentileStr.trim());
        return lookupPercentile;
    }

    public String getTargetScoreDerivationValue(Map<Integer, List<Double>> minMax) {
        if (CollectionUtils.isNotEmpty(existingPercentiles)) {
            Log.info("Target score derivation already exist!");
            return null;
        }
        List<BucketRange> percentiles = new ArrayList<>();
        for (int pct = percentileCalculator.getMinPct(); pct <= percentileCalculator.getMaxPct(); ++pct) {
            List<Double> vals = minMax.get(pct);
            if (CollectionUtils.isEmpty(vals))
                continue;
            Double curLower = vals.get(0);
            Double curUpper = vals.get(1);
            BucketRange range = new BucketRange();
            range.name = pct + "";
            range.lower = curLower;
            range.upper = curUpper;
            percentiles.add(range);
        }

        if (CollectionUtils.isEmpty(percentiles)) {
            log.warn("Can not create new target score derivation!");
            return null;
        }

        for (int i = 0; i < percentiles.size() - 1; i++) {
            percentiles.get(i).upper = percentiles.get(i + 1).lower;
        }
        ScoreDerivation derivation = new ScoreDerivation("TargetScoreDerivation", 0, percentiles, null);
        return JsonUtils.serialize(derivation);
    }

}
