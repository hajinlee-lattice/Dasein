package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.model.impl.pmmlresult.PMMLResultHandler;
import com.latticeengines.scoringapi.exposed.model.impl.pmmlresult.PMMLResultHandlerBase;

public class PMMLModelEvaluator extends DefaultModelEvaluator {

    private static final Logger log = LoggerFactory.getLogger(PMMLModelEvaluator.class);

    public PMMLModelEvaluator(InputStream is) {
        super(is);
    }

    public PMMLModelEvaluator(Reader pmml) {
        super(pmml);
    }

    @Override
    protected void calculatePercentile(Evaluator evaluator, ScoreDerivation derivation, Map<FieldName, ?> results,
            Map<ScoreType, Object> result) {
        String target = results.keySet().iterator().next().getValue();

        Object o = results.get(new FieldName(target));
        PMMLResultHandler handler = PMMLResultHandlerBase.getHandler(o.getClass());

        if (handler == null) {
            log.error(String.format("No handler for %s class type.", o.getClass().toString()));
            result.put(ScoreType.PERCENTILE, -1);
        } else {
            handler.processResult(evaluator, result, o);
        }
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
            throw new RuntimeException(e);
        }
    }

    private Object getDefaultValue(DataType dataType) {
        return DataTypeDefault.getByDataType(dataType).getDefaultValue();
    }

    @Override
    public Map<ScoreType, Object> evaluate(Map<String, Object> record, //
            ScoreDerivation derivation) {
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        Evaluator evaluator = modelEvaluatorFactory.newModelManager(manager.getPMML());

        Map<FieldName, FieldValue> arguments = new HashMap<FieldName, FieldValue>();
        boolean debugRow = false;

        for (FieldName name : evaluator.getActiveFields()) {
            Object value = record.get(name.getValue());

            if (value == null) {
                DataField dataField = evaluator.getDataField(name);

                if (dataField != null) {
                    value = getDefaultValue(dataField.getDataType());
                }
            }
            prepare(evaluator, arguments, debugRow, name, value);
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

        calculatePercentile(evaluator, derivation, results, result);

        return result;
    }

    private enum DataTypeDefault {

        STRING(DataType.STRING, ""), //
        INTEGER(DataType.INTEGER, 0), //
        FLOAT(DataType.FLOAT, 0.0), //
        DOUBLE(DataType.DOUBLE, 0.0), //
        BOOLEAN(DataType.BOOLEAN, false), //
        DATE(DataType.DATE, null), //
        TIME(DataType.TIME, null), //
        DATE_TIME(DataType.DATE_TIME, null), //
        DATE_DAYS_SINCE_0(DataType.DATE_DAYS_SINCE_0, null), //
        DATE_DAYS_SINCE_1960(DataType.DATE_DAYS_SINCE_1960, null), //
        DATE_DAYS_SINCE_1970(DataType.DATE_DAYS_SINCE_1970, null), //
        DATE_DAYS_SINCE_1980(DataType.DATE_DAYS_SINCE_1980, null),
        TIME_SECONDS(DataType.TIME_SECONDS, null),
        DATE_TIME_SECONDS_SINCE_0(DataType.DATE_TIME_SECONDS_SINCE_0, null),
        DATE_TIME_SECONDS_SINCE_1960(DataType.DATE_TIME_SECONDS_SINCE_1960, null),
        DATE_TIME_SECONDS_SINCE_1970(DataType.DATE_TIME_SECONDS_SINCE_1970, null),
        DATE_TIME_SECONDS_SINCE_1980(DataType.DATE_TIME_SECONDS_SINCE_1980, null);

        private final DataType dataType;
        private final Object defaultValue;

        private static Map<DataType, DataTypeDefault> map = new HashMap<>();

        static {
            for (DataTypeDefault d : DataTypeDefault.values()) {
                map.put(d.getDataType(), d);
            }
        }

        DataTypeDefault(DataType dataType, Object defaultValue) {
            this.dataType = dataType;
            this.defaultValue = defaultValue;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public DataType getDataType() {
            return dataType;
        }

        public static DataTypeDefault getByDataType(DataType dataType) {
            return map.get(dataType);
        }

    }
}
