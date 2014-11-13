package com.latticeengines.skald;

import java.io.StringReader;
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
import org.springframework.stereotype.Service;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

@Service
public class ModelEvaluator {
    public Map<String, Object> evaluate(ModelElement element, Map<String, Object> record) {
        PMML pmml;
        try {
            pmml = IOUtil.unmarshal(new InputSource(new StringReader(element.model.pmml)));
        } catch (SAXException | JAXBException e) {
            throw new RuntimeException(e);
        }

        PMMLManager manager = new PMMLManager(pmml);
        Evaluator evaluator = (Evaluator) manager.getModelManager(null, ModelEvaluatorFactory.getInstance());

        Map<FieldName, FieldValue> arguments = new HashMap<FieldName, FieldValue>();
        for (FieldName name : evaluator.getActiveFields()) {
            FieldValue value = evaluator.prepare(name, record.get(name));
            arguments.put(name, value);
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        Object predicted = EvaluatorUtil.decode(results.get(element.model.output));

        // TODO Create derived score elements.

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("probability", predicted);
        result.put("lift", 3.5);
        result.put("percentile", 96);
        result.put("bucket", "A");
        result.put("fake", true);
        return result;
    }
}
