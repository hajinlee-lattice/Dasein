package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.PMMLManager;
import org.jpmml.evaluator.ProbabilityDistribution;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.BucketRange;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;

public class DefaultModelEvaluator implements ModelEvaluator {
    private static final Logger log = LoggerFactory.getLogger(ModelEvaluator.class);

    protected final PMMLManager manager;

    private static final double DEFAULT_DOUBLE_VALUE = 0.0d;

    public DefaultModelEvaluator(InputStream is) {
        PMML unmarshalled;
        try {
            Source source = ImportFilter.apply(new InputSource(is));
            unmarshalled = JAXBUtil.unmarshalPMML(source);
        } catch (JAXBException | SAXException ex) {
            throw new RuntimeException("Unable to parse PMML file", ex);
        }

        this.manager = new PMMLManager(unmarshalled);
    }

    public DefaultModelEvaluator(Reader pmml) {
        PMML unmarshalled;
        try {
            Source source = ImportFilter.apply(new InputSource(pmml));
            unmarshalled = JAXBUtil.unmarshalPMML(source);
        } catch (JAXBException | SAXException ex) {
            throw new RuntimeException("Unable to parse PMML file", ex);
        }

        this.manager = new PMMLManager(unmarshalled);
    }

    @Override
    public Map<ScoreType, Object> evaluate(Map<String, Object> record, //
            ScoreDerivation derivation) {
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        Evaluator evaluator = modelEvaluatorFactory.newModelManager(manager.getPMML());

        Map<FieldName, FieldValue> arguments = new HashMap<FieldName, FieldValue>();
        List<String> nullFields = new ArrayList<>();

        boolean debugRow = false;

        if (log.isDebugEnabled()) {
            String recordKeyColumn = System.getProperty("ID");
            String recordKeyValue = System.getProperty("VALUE");
            if (recordKeyColumn != null && recordKeyValue != null) {
                Object v = record.get(recordKeyColumn);

                if (v != null) {
                    Double k = Double.valueOf(v.toString());

                    if (k.equals(Double.valueOf(recordKeyValue))) {
                        debugRow = true;
                    }
                }
            }
        }
        for (FieldName name : evaluator.getActiveFields()) {
            Object value = record.get(name.getValue());
            if (value == null) {
                nullFields.add(name.getValue());
                /*
                 * Set this in order to get through prepare so that we can
                 * collect all null fields before throwing an exception
                 */
                value = DEFAULT_DOUBLE_VALUE;
            }
            if (value instanceof Boolean) {
                Boolean booleanValue = ((Boolean) value).booleanValue();
                if (booleanValue)
                    value = new Double("1.0");
                else
                    value = new Double("0.0");
            }
            if (value instanceof BigInteger) {
                value = ((BigInteger) value).doubleValue();
            }
            if (value instanceof Long) {
                value = ((Long) value).doubleValue();
            }
            if (value instanceof Integer) {
                value = ((Integer) value).doubleValue();
            }
            prepare(evaluator, arguments, debugRow, name, value);
        }

        checkNullFields(nullFields);

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

    protected void prepare(Evaluator evaluator, Map<FieldName, FieldValue> arguments, boolean debugRow, FieldName name,
            Object value) {
        try {
            if (debugRow) {
                System.out.println(String.format("%s=%f", name, value));
            }
            arguments.put(name, evaluator.prepare(name, value));
        } catch (Exception e) {
            throw new ScoringApiException(LedpCode.LEDP_31103, new String[] { name.getValue(), String.valueOf(value) });
        }
    }

    protected boolean shouldThrowExceptionForNullFields() {
        return true;
    }

    protected void calculatePercentile(Evaluator evaluator, ScoreDerivation derivation, //
            Map<FieldName, ?> results, //
            Map<ScoreType, Object> result) {
        String target = derivation.target;
        ProbabilityDistribution classification = getClassification(results, target);
        double predicted = classification.getProbability("1");

        result.put(ScoreType.PROBABILITY_OR_VALUE, predicted);

        if (derivation.averageProbability != 0) {
            result.put(ScoreType.LIFT, predicted / derivation.averageProbability);
        }

        if (derivation.percentiles == null) {
            throw new ScoringApiException(LedpCode.LEDP_31011);
        } else if (derivation.percentiles.size() != 100) {
            log.warn(String.format("Not 100 buckets in score derivation. size:%d percentiles:%s",
                    derivation.percentiles.size(), JsonUtils.serialize(derivation.percentiles)));
        } else {
            double lowest = 1.0;
            double highest = 0.0;
            for (int index = 0; index < derivation.percentiles.size(); index++) {
                BucketRange percentileRange = derivation.percentiles.get(index);
                if (percentileRange.lower < lowest) {
                    lowest = percentileRange.lower;
                } else if (percentileRange.upper > highest) {
                    highest = percentileRange.upper;
                }
                if (withinRange(percentileRange, predicted)) {
                    // Name of the percentile bucket is the percentile value.
                    result.put(ScoreType.PERCENTILE, Integer.valueOf(percentileRange.name));
                    break;
                }
            }
            if (!result.containsKey(ScoreType.PERCENTILE)) {
                if (predicted <= lowest) {
                    result.put(ScoreType.PERCENTILE, 1);
                } else if (predicted >= highest) {
                    result.put(ScoreType.PERCENTILE, 100);
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
    }

    protected ProbabilityDistribution getClassification(Map<FieldName, ?> results, String target) {
        ProbabilityDistribution classification = null;
        if (target == null) {
            for (Map.Entry<FieldName, ?> entry : results.entrySet()) {
                if (entry.getValue() instanceof ProbabilityDistribution) {
                    classification = (ProbabilityDistribution) entry.getValue();
                    break;
                }
            }
        } else {
            classification = (ProbabilityDistribution) results.get(new FieldName(target));
        }
        return classification;
    }

    private boolean withinRange(BucketRange range, //
            double value) {
        return (range.lower == null || value >= range.lower) && (range.upper == null || value <= range.upper);
    }

    private void checkNullFields(List<String> nullFields) {
        String nullFieldsMsg = "";
        if (!nullFields.isEmpty()) {
            nullFieldsMsg = Joiner.on(",").join(nullFields);
            log.warn("Preevaluated fields with null values:" + nullFieldsMsg);
            if (shouldThrowExceptionForNullFields()) {
                throw new ScoringApiException(LedpCode.LEDP_31104, new String[] { nullFieldsMsg });
            }
        }
    }
}
