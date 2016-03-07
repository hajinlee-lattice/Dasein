package com.latticeengines.scoringapi.model;

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.python.jline.internal.Log;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.scoringapi.BucketRange;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.warnings.Warning;
import com.latticeengines.scoringapi.warnings.WarningCode;
import com.latticeengines.scoringapi.warnings.Warnings;

public class ModelEvaluator {

    private final PMMLManager manager;

    private Warnings warnings;

    public ModelEvaluator(InputStream is, Warnings warnings) {
        this.warnings = warnings;
        PMML unmarshalled;
        try {
            unmarshalled = IOUtil.unmarshal(is);
        } catch (JAXBException | SAXException ex) {
            throw new RuntimeException("Unable to parse PMML file", ex);
        }

        this.manager = new PMMLManager(unmarshalled);
    }

    public ModelEvaluator(Reader pmml) {
        PMML unmarshalled;
        try {
            unmarshalled = IOUtil.unmarshal(new InputSource(pmml));
        } catch (SAXException | JAXBException ex) {
            throw new RuntimeException("Unable to parse PMML file", ex);
        }

        this.manager = new PMMLManager(unmarshalled);
    }

    public Map<ScoreType, Object> evaluate(Map<String, Object> record, ScoreDerivation derivation,
            Map<String, FieldSchema> fieldSchemas) {
        Evaluator evaluator = (Evaluator) manager.getModelManager(null, ModelEvaluatorFactory.getInstance());

        Map<FieldName, FieldValue> arguments = new HashMap<FieldName, FieldValue>();
        List<String> nullFields = new ArrayList<>();
        for (FieldName name : evaluator.getActiveFields()) {
            Object value = record.get(name.getValue());
            if (value == null) {
                nullFields.add(name.getValue());
                FieldSchema schema = fieldSchemas.get(name.getValue());
                switch (schema.type) {
                case BOOLEAN:
                    value = 0.0d;
                    break;
                case INTEGER:
                case FLOAT:
                case LONG:
                case TEMPORAL:
                    value = 0.0d;
                    break;
                case STRING:
                    value = 0.0d;
                default:
                    break;
                }
            }
            if (value instanceof Long) {
                value = ((Long) value).doubleValue();
            }
            if (value instanceof Integer) {
                value = ((Integer) value).doubleValue();
            }
            try {
                arguments.put(name, evaluator.prepare(name, value));
            } catch (Exception e) {
                throw new ScoringApiException(LedpCode.LEDP_31103, new String[] { name.getValue(), String.valueOf(value) });
            }
        }
        if (!nullFields.isEmpty()) {
            String joinedNullFields = Joiner.on(",").join(nullFields);
            Log.info("Fields with null values:" + joinedNullFields);
            warnings.addWarning(new Warning(WarningCode.MISSING_VALUE, new String[] { joinedNullFields }));
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
        double predicted = classification.get("1");

        Map<ScoreType, Object> result = new HashMap<ScoreType, Object>();
        result.put(ScoreType.PROBABILITY, predicted);

        if (derivation.averageProbability != 0) {
            result.put(ScoreType.LIFT, predicted / derivation.averageProbability);
        }

        if (derivation.percentiles != null) {
            for (int index = 0; index < derivation.percentiles.size(); index++) {
                if (withinRange(derivation.percentiles.get(index), predicted)) {
                    result.put(ScoreType.PERCENTILE, index);
                    break;
                }
            }
        }

        if (derivation.buckets != null) {
            for (BucketRange range : derivation.buckets) {
                if (withinRange(range, predicted)) {
                    result.put(ScoreType.BUCKET, range.name);
                    break;
                }
            }
        }

        return result;
    }

    private boolean withinRange(BucketRange range, double value) {
        return (range.lower == null || value >= range.lower) && (range.upper == null || value < range.upper);
    }

}
