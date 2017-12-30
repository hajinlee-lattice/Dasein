package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cascading.tuple.Fields;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ContactNameConcatenateFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.ContactNameConcatenateParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(ContactNameConcatenate.DATAFLOW_BEAN_NAME)
public class ContactNameConcatenate
        extends TransformationFlowBase<BasicTransformationConfiguration, ContactNameConcatenateParameters> {
    public static final String DATAFLOW_BEAN_NAME = "ContactNameConcatenateFlow";

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(ContactNameConcatenateParameters parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));
        List<String> allNodeFields = node.getFieldNames();

        List<FieldMetadata> targetMetadata = new ArrayList<>();
        for (String field : parameters.getRetainFields()) {
            targetMetadata.add(new FieldMetadata(field, String.class));
        }
        targetMetadata.add(new FieldMetadata(parameters.getResultField(), String.class));

        if (!allNodeFields.contains(parameters.getResultField())) {
            node = node.addColumnWithFixedValue(parameters.getResultField(), null, String.class);
        }

        Fields fieldDeclaration = new Fields(node.getFieldNamesArray());

        return node.apply(
                new ContactNameConcatenateFunction(fieldDeclaration,
                        Arrays.asList(parameters.getRetainFields()),
                        Arrays.asList(parameters.getConcatenateFields()),
                        parameters.getResultField()),
                new FieldList(node.getFieldNames()),
                targetMetadata,
                new FieldList(node.getFieldNames()),
                Fields.REPLACE);
    }
}
