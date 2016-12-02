package com.latticeengines.domain.exposed.pmml;

import java.util.Deque;
import java.util.List;
import java.util.Objects;

import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.dmg.pmml.PMMLObject;
import org.dmg.pmml.Target;
import org.dmg.pmml.Targets;
import org.dmg.pmml.VisitorAction;
import org.jpmml.evaluator.IndexableUtil;
import org.jpmml.evaluator.UnsupportedFeatureException;
import org.jpmml.model.visitors.AbstractModelVisitor;

public class RegressionTargetCorrector extends AbstractModelVisitor {

    private Target.CastInteger castInteger = null;

    public RegressionTargetCorrector() {
        this(Target.CastInteger.ROUND);
    }

    public RegressionTargetCorrector(Target.CastInteger castInteger) {
        setCastInteger(Objects.requireNonNull(castInteger));
    }

    @Override
    public VisitorAction visit(Model model) {
        MiningFunctionType miningFunction = model.getFunctionName();

        switch (miningFunction) {
        case REGRESSION:
            processRegressionModel(model);
            break;
        default:
            break;
        }

        return VisitorAction.CONTINUE;
    }

    private void processRegressionModel(Model model) {
        PMML pmml = getPMML();

        MiningField miningField = getTargetField(model);
        if (miningField == null) {
            return;
        }

        FieldName name = miningField.getName();

        DataDictionary dataDictionary = pmml.getDataDictionary();

        DataField dataField = IndexableUtil.find(name, dataDictionary.getDataFields());
        if (dataField == null) {
            throw new RuntimeException(String.format("Missing field %s.", name));
        }

        DataType dataType = dataField.getDataType();
        switch (dataType) {
        case INTEGER:
            break;
        case FLOAT:
        case DOUBLE:
            return;
        default:
            throw new UnsupportedFeatureException(dataField, dataType);
        }

        Targets targets = model.getTargets();

        if (targets != null) {
            Target target = IndexableUtil.find(name, targets.getTargets());

            if (target != null) {

                if (target.getCastInteger() != null) {
                    return;
                } else

                {
                    target.setCastInteger(getCastInteger());
                }
            } else

            {
                targets.addTargets(createTarget(name));
            }
        } else

        {
            targets = new Targets().addTargets(createTarget(name));

            model.setTargets(targets);
        }
    }

    private Target createTarget(FieldName name) {
        Target target = new Target().setField(name).setCastInteger(getCastInteger());

        return target;
    }

    private PMML getPMML() {
        Deque<PMMLObject> parents = getParents();

        return (PMML) parents.getLast();
    }

    public Target.CastInteger getCastInteger() {
        return this.castInteger;
    }

    private void setCastInteger(Target.CastInteger castInteger) {
        this.castInteger = castInteger;
    }

    static private MiningField getTargetField(Model model) {
        MiningSchema miningSchema = model.getMiningSchema();

        MiningField result = null;

        List<MiningField> miningFields = miningSchema.getMiningFields();
        for (MiningField miningField : miningFields) {
            FieldUsageType fieldUsage = miningField.getUsageType();

            switch (fieldUsage) {
            case TARGET:
            case PREDICTED:
                if (result != null) {
                    throw new UnsupportedFeatureException(miningSchema);
                }
                result = miningField;
                break;
            default:
                break;
            }
        }

        return result;
    }
}