package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ContactNameConcatenateFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ContactNameConcatenateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

import cascading.tuple.Fields;

@Component(ContactNameConcatenate.DATAFLOW_BEAN_NAME)
public class ContactNameConcatenate extends ConfigurableFlowBase<ContactNameConcatenateConfig> {
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_CONTACT_NAME_CONCATENATER;
    public static final String DATAFLOW_BEAN_NAME = "ContactNameConcatenateFlow";

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ContactNameConcatenateConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        ContactNameConcatenateConfig config = getTransformerConfig(parameters);

        Node node = addSource(parameters.getBaseTables().get(0));
        if (!node.getFieldNames().contains(config.getResultField())) {
            node = node.addColumnWithFixedValue(config.getResultField(), null, String.class);
        }

        Fields fieldDeclaration = new Fields(node.getFieldNamesArray());

        return node.apply(
                new ContactNameConcatenateFunction(fieldDeclaration, Arrays.asList(config.getConcatenateFields()),
                        config.getResultField()), //
                new FieldList(node.getFieldNames()), //
                node.getSchema(), //
                new FieldList(node.getFieldNames()), //
                Fields.REPLACE);
    }
}
