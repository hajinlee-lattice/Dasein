package com.latticeengines.scoring.workflow.steps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

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
}
