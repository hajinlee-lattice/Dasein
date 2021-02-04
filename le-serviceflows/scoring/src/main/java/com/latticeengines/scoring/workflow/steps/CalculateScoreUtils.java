package com.latticeengines.scoring.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.spark.SparkConfigUtils;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;

public class CalculateScoreUtils {

    private static final Logger log = LoggerFactory.getLogger(CalculateScoreUtils.class);

    private CalculateScoreUtils() {
    }

    public static void writeTargetScoreDerivations(SparkJobResult result, CustomerSpace customerSpace,
            Configuration yarnConfiguration, ModelSummaryProxy modelSummaryProxy) {
        if (StringUtils.isNotBlank(result.getOutput())) {
            Map<String, String> targetScoreDerivationOutputs = new HashMap<>();
            Map<String, String> derivationMap = JsonUtils.deserialize(result.getOutput(),
                    new TypeReference<Map<String, String>>() {
                    });
            for (String modelId : derivationMap.keySet()) {
                ScoreDerivation der = JsonUtils.deserialize(derivationMap.get(modelId), ScoreDerivation.class);
                targetScoreDerivationOutputs.put(modelId, JsonUtils.serialize(der));
            }
            ExpectedRevenueDataFlowUtil.writeTargetScoreFiDerivationOutputs( //
                    customerSpace, yarnConfiguration, modelSummaryProxy, targetScoreDerivationOutputs);
        }
    }

    public static SparkJobResult mergeResult(List<SparkJobResult> results, String workspace,
            Configuration yarnConfiguration) {
        if (results.size() == 1) {
            return results.get(0);
        }
        List<HdfsDataUnit> tgtDataUnits = SparkConfigUtils.getTargetUnits(workspace, null, 1);
        long total = 0;
        for (int i = 0; i < results.size(); i++) {
            SparkJobResult result = results.get(i);
            try {
                SparkUtils.moveAvroParquetFiles(yarnConfiguration, result.getTargets().get(0).getPath(),
                        tgtDataUnits.get(0).getPath(), "batch" + i + "_");
                total += result.getTargets().get(0).getCount();
            } catch (Exception ex) {
                throw new RuntimeException("Can not merge data unit file!", ex);
            }
        }
        tgtDataUnits.get(0).setCount(total);
        SparkJobResult jobResult = new SparkJobResult();
        jobResult.setTargets(tgtDataUnits);
        return jobResult;
    }

    public static List<Map<String, String>> batchScoreFieldMaps(Map<String, String> allScoreFieldMap, String modelGuid,
            String rawScoreField, int batchSize) {
        List<Map<String, String>> scoreFieldMaps = new ArrayList<>();
        Map<String, String> scoreFieldMapBatch = new HashMap<>();
        if (MapUtils.isNotEmpty(allScoreFieldMap)) {
            for (String modelId : allScoreFieldMap.keySet()) {
                String scoreField = allScoreFieldMap.get(modelId);
                scoreFieldMapBatch.put(modelId, scoreField);
                if (scoreFieldMapBatch.size() >= batchSize) {
                    scoreFieldMaps.add(scoreFieldMapBatch);
                    scoreFieldMapBatch = new HashMap<>();
                }
            }
            if (scoreFieldMapBatch.size() > 0) {
                scoreFieldMaps.add(scoreFieldMapBatch);
            }
        } else if (StringUtils.isNotBlank(modelGuid)) {
            log.info(String.format("Using individual modelGuid %s to set scoreFieldMap", modelGuid));
            scoreFieldMapBatch.put(modelGuid, rawScoreField);
            scoreFieldMaps.add(scoreFieldMapBatch);
        } else {
            throw new RuntimeException("Couldn't find any valid scoreFieldMap or individual modelGuid");
        }
        return scoreFieldMaps;
    }
}
