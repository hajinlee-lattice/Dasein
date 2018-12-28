package com.latticeengines.scoring.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

public abstract class ScoringServiceFlowsDataFlowFunctionalTestNGBase extends ServiceFlowsDataFlowFunctionalTestNGBase {

    protected void setFitFunctionParametersMap(CalculateExpectedRevenuePercentileParameters parameters) {
        Map<String, String> fitFunctionParametersMap = new HashMap<String, String>();
        String evFitFunctions = "{\n    "
                + "\"ev\": {\n        \"alpha\": -4.312307952765219e-09, \n        \"beta\": 9.816450543885734, \n        \"gamma\": 0, \n        \"maxRate\": 18332.845068510658, \n        \"version\": \"v2\"\n    }, \n    "
                + "\"revenue\": {\n        \"alpha\": 0.0, \n        \"beta\": 5.997588369855518, \n        \"gamma\": 0.0, \n        \"maxRate\": 1120.5, \n        \"version\": \"v2\"\n    }, \n    "
                + "\"probability\": {\n        \"alpha\": -0.27206272066718284, \n        \"beta\": -3.2757173886422875, \n        \"gamma\": -0.9, \n        \"maxRate\": 0.16666666666666666, \n        \"version\": \"v2\"\n    }\n}";
        parameters.getOriginalScoreFieldMap() //
                .keySet().stream() //
                .filter(k -> ScoreResultField.ExpectedRevenue.displayName
                        .equals(parameters.getOriginalScoreFieldMap().get(k))) //
                .forEach(k -> fitFunctionParametersMap.put(k, evFitFunctions));
        String nonEVFitFunctions = "{\n    "
                + "\"probability\": {\n        \"alpha\": -0.27206272066718284, \n        \"beta\": -3.2757173886422875, \n        \"gamma\": -0.9, \n        \"maxRate\": 0.16666666666666666, \n        \"version\": \"v2\"\n    }\n}";
        parameters.getOriginalScoreFieldMap() //
                .keySet().stream() //
                .filter(k -> !ScoreResultField.ExpectedRevenue.displayName
                        .equals(parameters.getOriginalScoreFieldMap().get(k))) //
                .forEach(k -> fitFunctionParametersMap.put(k, nonEVFitFunctions));
        parameters.setFitFunctionParametersMap(fitFunctionParametersMap);
    }

}
