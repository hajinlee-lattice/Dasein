package com.latticeengines.skald;

import java.util.HashMap;
import java.util.Map;

import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.skald.model.ScoreDerivation;

@Service
public class ModelEvaluator {
    public Map<String, Object> evaluate(CustomerSpace space, String model, ScoreDerivation derivation,
            Map<String, Object> record) {
        PMMLManager manager = retriever.getModel(space, model);
        Evaluator evaluator = (Evaluator) manager.getModelManager(null, ModelEvaluatorFactory.getInstance());

        Map<FieldName, FieldValue> arguments = new HashMap<FieldName, FieldValue>();
        for (FieldName name : evaluator.getActiveFields()) {
            FieldValue value = evaluator.prepare(name, record.get(name));
            arguments.put(name, value);
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        Object predicted = EvaluatorUtil.decode(results.get("XXX target field"));

        // TODO Create derived score elements.

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("probability", predicted);
        result.put("lift", 3.5);
        result.put("percentile", 96);
        result.put("bucket", "A");
        result.put("fake", true);
        return result;
    }

    @Autowired
    private ModelRetriever retriever;
}
