package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DeriveAttributeFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DeriveAttributeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenFuncConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("deriveAttributeFlow")
public class DeriveAttributeFlow extends TblDrivenFlowBase<DeriveAttributeConfig, DeriveAttributeConfig.DeriveFunc> {

    private static final Logger log = LoggerFactory.getLogger(DeriveAttributeFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        DeriveAttributeConfig config = getTransformerConfig(parameters);

        List<DeriveAttributeConfig.DeriveFunc> attributes = getAttributeFuncs(config);

        Node source = addSource(parameters.getBaseTables().get(0));

        List<String> fieldNames = source.getFieldNames();

        for (String fieldName : fieldNames) {
            log.info("Field in input schema " + fieldName);
        }
        FieldList fieldsToApply = new FieldList(fieldNames);
        List<String> outputFieldNames = new ArrayList<>(fieldNames);
        List<String> newFieldNames = new ArrayList<>();
        List<FieldMetadata> targetFields = new ArrayList<>();

        for (DeriveAttributeConfig.DeriveFunc deriveFunc : attributes) {
             String target = deriveFunc.getTarget();
             outputFieldNames.add(target);
             newFieldNames.add(target);
             targetFields.add(new FieldMetadata(target, deriveFunc.getTypeClass()));
        }
        FieldList outputFields = new FieldList(outputFieldNames);

        Fields funcFields = new Fields(newFieldNames.toArray(new String[newFieldNames.size()]));
        DeriveAttributeFunction function = new DeriveAttributeFunction(funcFields, attributes);

        Node derived = source.apply(function, fieldsToApply, targetFields, outputFields);
        return derived;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return DeriveAttributeConfig.class;
    }

    @Override
    public Class<? extends TblDrivenFuncConfig> getTblDrivenFuncConfigClass() {
        return DeriveAttributeConfig.DeriveFunc.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "deriveAttributeFlow";
    }

    @Override
    public String getTransformerName() {
        return "deriveAttribute";

    }
}
