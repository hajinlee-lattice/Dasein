package com.latticeengines.dataflow.runtime.cascading.cdl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.BucketRange;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("rawtypes")
public class CalculatePercentile extends BaseOperation implements Buffer {

    private static final Logger log = LoggerFactory.getLogger(CalculatePercentile.class);
    private static final long serialVersionUID = -7641395420638947868L;
    private int minPct;
    private int maxPct;
    private String countFieldName;
    private int scoreFieldPos;
    private String rawScoreFieldName;
    private boolean targetScoreDerivation;
    private Map<String, String> targetScoreDerivationPaths;
    private String modelGuidFieldName;
    private String modelId;
    private List<BucketRange> percentiles;

    public CalculatePercentile(Fields fieldDescription, int minPct, int maxPct, String scoreFieldName,
            String countFieldName, String rawScoreFieldName, boolean targetScoreDerivation,
            Map<String, String> targetScoreDerivationPaths, String modelGuidFieldName) {
        super(fieldDescription);

        this.minPct = minPct;
        this.maxPct = maxPct;
        this.countFieldName = countFieldName;
        this.rawScoreFieldName = rawScoreFieldName;
        this.scoreFieldPos = fieldDescription.getPos(scoreFieldName);
        this.targetScoreDerivation = targetScoreDerivation;
        this.targetScoreDerivationPaths = targetScoreDerivationPaths;
        this.modelGuidFieldName = modelGuidFieldName;

        if (scoreFieldPos == -1) {
            throw new RuntimeException("Cannot find field " + scoreFieldName + " from metadata");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntryCollector collector = bufferCall.getOutputCollector();
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();

        SimplePercentileCalculator percentileCalculator = new SimplePercentileCalculator(minPct, maxPct);

        int currentPos = 0;
        modelId = null;
        percentiles = null;
        while (iter.hasNext()) {
            TupleEntry tuple = iter.next();
            long totalCount = tuple.getLong(countFieldName);
            double rawScore = tuple.getDouble(rawScoreFieldName);
            if (targetScoreDerivation && lookupScore(collector, tuple, rawScore)) {
                continue;
            }

            Integer pct = percentileCalculator.compute(totalCount, currentPos, rawScore);
            Tuple result = tuple.getTupleCopy();
            result.set(scoreFieldPos, pct);
            collector.add(result);
            currentPos++;
        }
        if (targetScoreDerivation && CollectionUtils.isEmpty(percentiles)) {
            writeTargetScoreDerivation(percentileCalculator, modelId);
        }
    }

    private boolean lookupScore(TupleEntryCollector collector, TupleEntry tuple, double rawScore) {
        boolean found = false;
        if (modelId == null) {
            modelId = tuple.getString(modelGuidFieldName);
            getPercentiles(modelId);
        }
        if (CollectionUtils.isNotEmpty(percentiles)) {
            Tuple result = tuple.getTupleCopy();
            Integer pct = lookup(rawScore);
            result.set(scoreFieldPos, pct);
            collector.add(result);
            found = true;
        }
        return found;
    }

    private Integer lookup(Double score) {
        String percentileStr = percentiles.stream() //
                .parallel() //
                .filter(p -> {
                    return p.lower <= score && score < p.upper;
                }) //
                .map(p -> {
                    return p.name;
                }) //
                .findFirst().orElse(null);

        if (StringUtils.isBlank(percentileStr)) {
            if (percentiles.get(percentiles.size() - 1).upper <= score) {
                percentileStr = percentiles.get(percentiles.size() - 1).name;
            } else if (percentiles.get(0).lower > score) {
                percentileStr = percentiles.get(0).name;
            } else {
                throw new RuntimeException();
            }
        }
        Integer lookupPercentile = Integer.parseInt(percentileStr.trim());
        return lookupPercentile;
    }

    private void getPercentiles(String modelId) {
        String path = targetScoreDerivationPaths.get(modelId);
        if (path == null) {
            log.warn("Can not find the target score derivation path for modelId=" + modelId);
            return;
        }
        try {
            String targetScoreDerivation = HdfsUtils.getHdfsFileContents(new Configuration(), path);
            if (targetScoreDerivation == null) {
                log.warn("There's no target score derivation path for modelId=" + modelId);
                return;
            }
            ScoreDerivation scoreDerivation = JsonUtils.deserialize(targetScoreDerivation, ScoreDerivation.class);
            if (scoreDerivation == null)
                return;
            percentiles = scoreDerivation.percentiles;
            log.info("Found target score percentile for modelId=" + modelId + " on path=" + path);
        } catch (Exception ex) {
            log.warn("Can not read target score derivation for modelId=" + modelId + " error=" + ex.getMessage());
        }
    }

    private void writeTargetScoreDerivation(SimplePercentileCalculator percentileCalculator, String modelId) {
        try {
            String scoreDerivation = createScoreDerivation(percentileCalculator);
            if (scoreDerivation != null) {
                String path = targetScoreDerivationPaths.get(modelId);
                if (path != null) {
                    log.info("Starting to write target score derivation to " + path + " for modelId=" + modelId);
                    HdfsUtils.writeToFile(new Configuration(), path, scoreDerivation);
                    log.info("Finished writing target score derivation to " + path + " for modelId=" + modelId);
                } else {
                    log.warn("Can not find the target score derivation path for modelId=" + modelId);
                }
            }
        } catch (Exception ex) {
            log.warn("Can not write target score derivation for modelId=" + modelId + " error=" + ex.getMessage(), ex);
        }
    }

    private String createScoreDerivation(SimplePercentileCalculator calculator) {
        List<BucketRange> percentiles = new ArrayList<>();
        for (int pct = calculator.getMinPct(); pct <= calculator.getMaxPct(); ++pct) {
            Double curLower = calculator.getLowerBound(pct);
            Double curUpper = calculator.getUpperBound(pct);
            if (curLower == null || curUpper == null)
                continue;
            BucketRange range = new BucketRange();
            range.name = pct + "";
            range.lower = curLower;
            range.upper = curUpper;
            percentiles.add(range);
        }
        if (CollectionUtils.isEmpty(percentiles)) {
            Log.warn("Can not create target score derivation!");
            return null;
        }
        for (int i = 0; i < percentiles.size() - 1; i++) {
            percentiles.get(i).upper = percentiles.get(i + 1).lower;
        }
        ScoreDerivation derivation = new ScoreDerivation("TargetScoreDerivation", 0, percentiles, null);
        return JsonUtils.serialize(derivation);
    }
}
