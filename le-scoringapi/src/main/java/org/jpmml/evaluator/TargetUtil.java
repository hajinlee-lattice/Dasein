/*
 * Copyright (c) 2013 Villu Ruusmann
 *
 * This file is part of JPMML-Evaluator
 *
 * JPMML-Evaluator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Evaluator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Evaluator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.evaluator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.Target;
import org.dmg.pmml.TargetValue;

public class TargetUtil {

    private TargetUtil() {
    }

    public static Map<FieldName, ?> evaluateRegressionDefault(ModelEvaluationContext context) {
        return evaluateRegression((Double) null, context);
    }

    public static Map<FieldName, ?> evaluateRegression(Double value, ModelEvaluationContext context) {
        ModelEvaluator<?> evaluator = context.getModelEvaluator();

        return evaluateRegression(evaluator.getTargetField(), value, context);
    }

    public static Map<FieldName, ?> evaluateRegression(FieldName name, Double value, ModelEvaluationContext context) {
        return Collections.singletonMap(name, evaluateRegressionInternal(name, value, context));
    }

    static Object evaluateRegressionInternal(FieldName name, Object value, ModelEvaluationContext context) {
        ModelEvaluator<?> evaluator = context.getModelEvaluator();

        if (Objects.equals(Evaluator.DEFAULT_TARGET, name)) {
            DataField dataField = evaluator.getDataField();

            if (value != null) {
                value = TypeUtil.cast(dataField.getDataType(), value);
            }
        } else

        {
            Target target = evaluator.getTarget(name);
            if (target != null) {

                if (value == null) {
                    value = getDefaultValue(target);
                } // End if

                if (value != null) {
                    value = processValue(target, (Double) value);
                }
            }

            DataField dataField = evaluator.getDataField(name);
            if (dataField == null) {
                throw new MissingFieldException(name);
            } // End if

            if (value != null) {
                value = TypeUtil.cast(dataField.getDataType(), value);
            }

            MiningField miningField = evaluator.getMiningField(name);

            context.declare(name, FieldValueUtil.prepareTargetValue(dataField, miningField, target, value));
        }

        return value;
    }

    public static Map<FieldName, ? extends Classification> evaluateClassificationDefault(
            ModelEvaluationContext context) {
        return evaluateClassification((Classification) null, context);
    }

    public static Map<FieldName, ? extends Classification> evaluateClassification(Classification value,
            ModelEvaluationContext context) {
        ModelEvaluator<?> evaluator = context.getModelEvaluator();

        return evaluateClassification(evaluator.getTargetField(), value, context);
    }

    public static Map<FieldName, ? extends Classification> evaluateClassification(FieldName name, Classification value,
            ModelEvaluationContext context) {
        return Collections.singletonMap(name, evaluateClassificationInternal(name, value, context));
    }

    static Classification evaluateClassificationInternal(FieldName name, Classification value,
            ModelEvaluationContext context) {
        ModelEvaluator<?> evaluator = context.getModelEvaluator();

        if (Objects.equals(Evaluator.DEFAULT_TARGET, name)) {
            DataField dataField = evaluator.getDataField();

            if (value != null) {
                value.computeResult(dataField.getDataType());
            }
        } else

        {
            Target target = evaluator.getTarget(name);
            if (target != null) {

                if (value == null) {
                    value = getPriorProbabilities(target);
                }
            }

            DataField dataField = evaluator.getDataField(name);
            // if(dataField == null){
            // throw new MissingFieldException(name);
            // } // End if
            if (dataField != null) {
                if (value != null) {
                    value.computeResult(dataField.getDataType());
                }

                MiningField miningField = evaluator.getMiningField(name);

                context.declare(name, FieldValueUtil.prepareTargetValue(dataField, miningField, target,
                        value != null ? value.getResult() : null));
            }
        }

        return value;
    }

    public static Double processValue(Target target, Double value) {
        double result = value;

        Double min = target.getMin();
        if (min != null) {
            result = Math.max(result, min);
        }

        Double max = target.getMax();
        if (max != null) {
            result = Math.min(result, max);
        }

        result = (result * target.getRescaleFactor()) + target.getRescaleConstant();

        Target.CastInteger castInteger = target.getCastInteger();
        if (castInteger == null) {

            if (result == value.doubleValue()) {
                return value;
            }

            return result;
        }

        switch (castInteger) {
        case ROUND:
            return (double) Math.round(result);
        case CEILING:
            return Math.ceil(result);
        case FLOOR:
            return Math.floor(result);
        default:
            throw new UnsupportedFeatureException(target, castInteger);
        }
    }

    public static TargetValue getTargetValue(Target target, Object value) {
        DataType dataType = TypeUtil.getDataType(value);

        List<TargetValue> targetValues = target.getTargetValues();
        for (TargetValue targetValue : targetValues) {

            if (TypeUtil.equals(dataType, value, TypeUtil.parseOrCast(dataType, targetValue.getValue()))) {
                return targetValue;
            }
        }

        return null;
    }

    static private Double getDefaultValue(Target target) {
        List<TargetValue> values = target.getTargetValues();

        if (values.isEmpty()) {
            return null;
        } // End if

        if (values.size() != 1) {
            throw new InvalidFeatureException(target);
        }

        TargetValue value = values.get(0);

        // "The value and priorProbability attributes are used only if the
        // optype of the field is categorical or ordinal"
        if (value.getValue() != null || value.getPriorProbability() != null) {
            throw new InvalidFeatureException(value);
        }

        return value.getDefaultValue();
    }

    static private ProbabilityDistribution getPriorProbabilities(Target target) {
        ProbabilityDistribution result = new ProbabilityDistribution();

        List<TargetValue> values = target.getTargetValues();
        for (TargetValue value : values) {

            // "The defaultValue attribute is used only if the optype of the
            // field is continuous"
            if (value.getDefaultValue() != null) {
                throw new InvalidFeatureException(value);
            }

            String targetCategory = value.getValue();
            Double probability = value.getPriorProbability();

            if (targetCategory == null || probability == null) {
                continue;
            }

            result.put(targetCategory, probability);
        }

        if (result.isEmpty()) {
            return null;
        }

        return result;
    }
}
