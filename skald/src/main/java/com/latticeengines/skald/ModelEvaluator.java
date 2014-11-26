package com.latticeengines.skald;

import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.ClassificationMap;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.latticeengines.domain.exposed.skald.model.ScoreDerivation;

public class ModelEvaluator {
    public ModelEvaluator(Reader pmml) {
        PMML unmarshalled;
        try {
            unmarshalled = IOUtil.unmarshal(new InputSource(pmml));
        } catch (SAXException | JAXBException ex) {
            throw new RuntimeException("Unable to parse PMML file", ex);
        }

        this.manager = new PMMLManager(unmarshalled);
    }

    public Map<ScoreType, Object> evaluate(Map<String, Object> record, ScoreDerivation derivation) {
        Evaluator evaluator = (Evaluator) manager.getModelManager(null, ModelEvaluatorFactory.getInstance());

        Map<FieldName, FieldValue> arguments = new HashMap<FieldName, FieldValue>();
        for (FieldName name : evaluator.getActiveFields()) {
            Object value = record.get(name.getValue());
            if (value == null) {
                throw new RuntimeException("Null value for model input " + name.getValue());
            }
            arguments.put(name, evaluator.prepare(name, value));
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);

        String target = derivation.target;
        if (target == null) {
            if (results.size() == 1) {
                target = results.keySet().iterator().next().getValue();
            } else {
                throw new RuntimeException("PMML model has multiple results and no target was specified");
            }
        }

        @SuppressWarnings("unchecked")
        ClassificationMap<FieldName> classification = (ClassificationMap<FieldName>) results.get(new FieldName(target));
        Object predicted = classification.get("1");

        // TODO Create derived score elements.

        Map<ScoreType, Object> result = new HashMap<ScoreType, Object>();
        result.put(ScoreType.PROBABILITY, predicted);
        result.put(ScoreType.LIFT, 3.5);
        result.put(ScoreType.PERCENTILE, 96);
        result.put(ScoreType.BUCKET, "A");
        return result;
    }

    private final PMMLManager manager;
}
