package com.latticeengines.skald;

import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.latticeengines.skald.model.ScoreDerivation;

public class ModelEvaluator {
    public ModelEvaluator(Reader pmml, ScoreDerivation derivation) {
        PMML unmarshalled;
        try {
            unmarshalled = IOUtil.unmarshal(new InputSource(pmml));
        } catch (SAXException | JAXBException ex) {
            throw new RuntimeException("Unable to parse PMML file", ex);
        }

        this.manager = new PMMLManager(unmarshalled);
        this.derivation = derivation;
    }

    public Map<String, Object> evaluate(Map<String, Object> record) {
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

    private final PMMLManager manager;
    private final ScoreDerivation derivation;
}
