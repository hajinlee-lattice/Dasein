package com.latticeengines.scoring.exposed.service.impl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.springframework.stereotype.Component;

import com.latticeengines.scoring.exposed.domain.ScoringRequest;
import com.latticeengines.scoring.exposed.domain.ScoringResponse;
import com.latticeengines.scoring.exposed.service.ScoringService;

@Component("scoringService")
public class ScoringServiceImpl implements ScoringService {

    @Override
    public List<ScoringResponse> scoreBatch(List<ScoringRequest> scoringRequests, PMML pmml) {
        return null;
    }

    @Override
    public ScoringResponse score(ScoringRequest scoringRequest, PMML pmml) {
        PMMLManager pmmlManager = new PMMLManager(pmml);
        Evaluator evaluator = (Evaluator) pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());
        Map<FieldName, Object> arguments = new LinkedHashMap<FieldName, Object>();

        List<FieldName> activeFields = evaluator.getActiveFields();
        for (FieldName activeField : activeFields) {
            Object value = scoringRequest.getArgument(activeField.getValue());
            arguments.put(activeField, EvaluatorUtil.prepare(evaluator, activeField, value));
        }

        Map<FieldName, ?> result = evaluator.evaluate(arguments);
        ScoringResponse response = new ScoringResponse();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        for (Entry<FieldName, ?> entry : result.entrySet()) {
            resultMap.put(entry.getKey().getValue(), entry.getValue());
        }
        response.setResult(resultMap);
        return response;
    }

}
