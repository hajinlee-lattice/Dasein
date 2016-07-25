package com.latticeengines.domain.exposed.util;

import java.util.List;

import com.latticeengines.domain.exposed.modelreview.DataRule;

public class DataRuleUtils {

    public static void populateDataRuleDisplayNameAndDescriptions(List<DataRule> dataRules) {
        for (DataRule rule : dataRules) {
            switch (rule.getName()) {
            case "UniqueValueCountDS":
                rule.setDescription("Remove categorical attributes that have more than 200 distinct values");
                rule.setDisplayName("Detected Too Many Distinct Values");
                break;
            case "PopulatedRowCountDS":
                rule.setDescription("Remove attributes when the predictive power comes from noise");
                rule.setDisplayName("Detected Sampling Noise");
                break;
            case "OverlyPredictiveDS":
                rule.setDescription("Remove attributes when excessive lift is detected for some data values");
                rule.setDisplayName("Detected Predictors Too Good to be True");
                break;
            case "LowCoverageDS":
                rule.setDescription("Remove attributes that have very low coverage");
                rule.setDisplayName("Detected Low Coverage in Attributes");
                break;
            case "NullIssueDS":
                rule.setDescription("Remove attributes when lift from NULL values is too strong compared to other values");
                rule.setDisplayName("Detected Attributes Over Using NULL Values");
                break;
            case "HighlyPredictiveSmallPopulationDS":
                rule.setDescription("Remove small groups of events that bias the model and overset lift expectation");
                rule.setDisplayName("Detected Noisy Spikes in Lift");
                break;

            default:
                break;
            }
        }
    }

}
