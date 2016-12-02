package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.model.impl.pmmlresult.PMMLResultHandler;
import com.latticeengines.scoringapi.exposed.model.impl.pmmlresult.PMMLResultHandlerBase;

public class PMMLModelEvaluator extends DefaultModelEvaluator {

    public PMMLModelEvaluator(InputStream is) {
        super(is);
    }

    public PMMLModelEvaluator(Reader pmml) {
        super(pmml);
    }

    @Override
    protected void calculatePercentile(ScoreDerivation derivation, Map<FieldName, ?> results,
            Map<ScoreType, Object> result) {
        String target = results.keySet().iterator().next().getValue();

        Object o = results.get(new FieldName(target));
        PMMLResultHandler handler = PMMLResultHandlerBase.getHandler(o.getClass());
        handler.processResult(result, o);
    }

    @Override
    protected boolean shouldThrowExceptionForNullFields() {
        return false;
    }

    @Override
    protected void prepare(Evaluator evaluator, Map<FieldName, FieldValue> arguments, boolean debugRow, FieldName name,
            Object value) {
        try {
            super.prepare(evaluator, arguments, debugRow, name, value);
        } catch (Exception e) {
            super.prepare(evaluator, arguments, debugRow, name, DEFAULT_DOUBLE_VALUE);
        }
    }
    
    @Override
    public Map<ScoreType, Object> evaluate(Map<String, Object> record, //
            ScoreDerivation derivation) {
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        Evaluator evaluator = modelEvaluatorFactory.newModelManager(manager.getPMML());

        Map<FieldName, FieldValue> arguments = new HashMap<FieldName, FieldValue>();
        boolean debugRow = false;

        for (FieldName name : evaluator.getActiveFields()) {
            prepare(evaluator, arguments, debugRow, name, record.get(name.getValue()));
        }

        Map<FieldName, ?> results = null;
        try {
            results = evaluator.evaluate(arguments);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31014, e, new String[] { JsonUtils.serialize(arguments) });
        }

        if (results == null) {
            throw new LedpException(LedpCode.LEDP_31013);
        }

        Map<ScoreType, Object> result = new HashMap<ScoreType, Object>();

        calculatePercentile(derivation, results, result);

        return result;
    }
}
