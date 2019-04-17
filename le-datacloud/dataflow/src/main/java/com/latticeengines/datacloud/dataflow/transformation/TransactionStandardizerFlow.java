package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.TransactionStandardizerFlow.BEAN_NAME;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.TransactionStandardizerFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransactionStandardizerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(BEAN_NAME)
public class TransactionStandardizerFlow extends ConfigurableFlowBase<TransactionStandardizerConfig> {
    public static final String BEAN_NAME = "transactionStandardizerFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        TransactionStandardizerConfig config = getTransformerConfig(parameters);
        List<String> sFields = config.getStringFields();
        List<String> lFields = config.getLongFields();
        List<String> iFields = config.getIntFields();
        String cField = config.getCustomField();

        Node node = addSource(parameters.getBaseTables().get(0));

        List<String> nFields = node.getFieldNames();
        Set<String> nFieldSet = new HashSet<String>(nFields);

        List<String> oldFields = new ArrayList<String>();
        List<String> newFields = new ArrayList<String>();
        List<String> customFields = new ArrayList<String>();

        List<String> fields = new ArrayList<String>();
        List<FieldMetadata> fms = new ArrayList<FieldMetadata>();
        for (String field : sFields) {
            fields.add(field);
            if (nFieldSet.contains(field)) {
                oldFields.add(field);
            } else {
                newFields.add(field);
                fms.add(new FieldMetadata(field, String.class));
            }
        }

        for (String field : lFields) {
            fields.add(field);
            if (nFieldSet.contains(field)) {
                oldFields.add(field);
            } else {
                newFields.add(field);
                fms.add(new FieldMetadata(field, Long.class));
            }
        }

        for (String field : iFields) {
            fields.add(field);
            if (nFieldSet.contains(field)) {
                oldFields.add(field);
            } else {
                newFields.add(field);
                fms.add(new FieldMetadata(field, Integer.class));
            }
        }
        Set<String> fieldSet = new HashSet<String>(fields);


        for (String field : nFields) {
            if (!fieldSet.contains(field)) {
                customFields.add(field);
            }
        }

        Fields fieldDeclaration = new Fields(fields.toArray(new String[fields.size()]));
        node = node.apply(
                new TransactionStandardizerFunction(fieldDeclaration, cField, oldFields, newFields, customFields), //
                new FieldList(node.getFieldNames()), //
                fms, //
                new FieldList(fields),
                Fields.RESULTS);

        return node;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TransactionStandardizerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.TRANSACTION_STANDARDIZER;
    }
}
