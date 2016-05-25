package com.latticeengines.scoringapi.exposed.model;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.PMMLManager;
import org.jpmml.evaluator.ProbabilityDistribution;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
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

public class ModelEvaluator {

    private static final Log log = LogFactory.getLog(ModelEvaluator.class);

    private final PMMLManager manager;

    public ModelEvaluator(InputStream is) {
        PMML unmarshalled;
        try {
            Source source = ImportFilter.apply(new InputSource(is));
            unmarshalled = JAXBUtil.unmarshalPMML(source);
        } catch (JAXBException | SAXException ex) {
            throw new RuntimeException("Unable to parse PMML file", ex);
        }

        this.manager = new PMMLManager(unmarshalled);
    }

    public ModelEvaluator(Reader pmml) {
        PMML unmarshalled;
        try {
            Source source = ImportFilter.apply(new InputSource(pmml));
            unmarshalled = JAXBUtil.unmarshalPMML(source);
        } catch (JAXBException | SAXException ex) {
            throw new RuntimeException("Unable to parse PMML file", ex);
        }

        this.manager = new PMMLManager(unmarshalled);
    }

    public Map<ScoreType, Object> evaluate(Map<String, Object> record, ScoreDerivation derivation) {
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
                value = 0.0d;
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
            try {
                if (debugRow) {
                    System.out.println(String.format("%s=%f", name, value));
                }
                arguments.put(name, evaluator.prepare(name, value));
            } catch (Exception e) {
                throw new ScoringApiException(LedpCode.LEDP_31103, new String[] { name.getValue(),
                        String.valueOf(value) });
            }
        }
        String nullFieldsMsg = "";
        if (!nullFields.isEmpty()) {
            nullFieldsMsg = Joiner.on(",").join(nullFields);
            log.warn("Preevaluated fields with null values:" + nullFieldsMsg);
            throw new ScoringApiException(LedpCode.LEDP_31104, new String[] { nullFieldsMsg });
        }

        Map<FieldName, ?> results = null;
        try {
            results = evaluator.evaluate(arguments);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31014, e, new String[] { JsonUtils.serialize(arguments) });
        }

        if (results == null) {
            throw new LedpException(LedpCode.LEDP_31013);
        } else if (results.size() != 1) {
            throw new LedpException(LedpCode.LEDP_31012, new String[] { String.valueOf(results.size()) });
        }

        String target = derivation.target;
        if (target == null) {
            target = results.keySet().iterator().next().getValue();
        }

        ProbabilityDistribution classification = (ProbabilityDistribution) results.get(new FieldName(target));
        double predicted = classification.getProbability("1");

        Map<ScoreType, Object> result = new HashMap<ScoreType, Object>();
        result.put(ScoreType.PROBABILITY, predicted);

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

        return result;
    }

    private boolean withinRange(BucketRange range, double value) {
        return (range.lower == null || value >= range.lower) && (range.upper == null || value < range.upper);
    }

}
