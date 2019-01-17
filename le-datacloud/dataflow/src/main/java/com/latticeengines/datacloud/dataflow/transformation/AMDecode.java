package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMDecodeFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AMDecoderParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.tuple.Fields;

@Component(AMDecode.DATAFLOW_BEAN_NAME)
public class AMDecode extends TransformationFlowBase<BasicTransformationConfiguration, AMDecoderParameters> {
    public static final String DATAFLOW_BEAN_NAME = "AMDecodeFlow";
    public static final String TRANSFORMER_NAME = "AMDecoderTransformer";

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(AMDecoderParameters parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));
        List<String> decodeAttributes = new ArrayList<>(parameters.getCodeBookLookup().keySet());
        List<String> encodeAttributes = new ArrayList<>(parameters.getCodeBookMap().keySet());
        List<String> fieldsToRetain = ArrayUtils.isEmpty(parameters.getRetainFields()) ? new ArrayList<>()
                : new ArrayList<>(Arrays.asList(parameters.getRetainFields()));
        if (parameters.isDecodeAll()) {
            List<String> allFields = new ArrayList<>(node.getFieldNames());
            fieldsToRetain = allFields.stream() //
                    // Retain all plain attributes
                    .filter(field -> !parameters.getCodeBookMap().containsKey(field)) //
                    .collect(Collectors.toList());
        }
        fieldsToRetain.addAll(encodeAttributes);
        node = node.retain(new FieldList(fieldsToRetain));

        List<FieldMetadata> targetMetadata = new ArrayList<>();
        for (String attribute : decodeAttributes) {
            String encoded = parameters.getCodeBookLookup().get(attribute);
            BitCodeBook codeBook = parameters.getCodeBookMap().get(encoded);
            switch (codeBook.getDecodeStrategy()) {
                case ENUM_STRING:
                case BOOLEAN_YESNO:
                    targetMetadata.add(new FieldMetadata(attribute, String.class));
                    break;
                case NUMERIC_INT:
                case NUMERIC_UNSIGNED_INT:
                    targetMetadata.add(new FieldMetadata(attribute, Integer.class));
                    break;
                default:
                    throw new RuntimeException(String.format("Decode strategy %s is not supported in AM decoding.",
                            codeBook.getDecodeStrategy()));
            }
        }

        List<String> outputFields = node.getFieldNames();
        outputFields.addAll(decodeAttributes);

        Fields fieldDeclaration = new Fields(decodeAttributes.toArray(new String[decodeAttributes.size()]));
        node = node.apply(
                new AMDecodeFunction(fieldDeclaration, parameters.getCodeBookLookup(), parameters.getCodeBookMap()),
                new FieldList(node.getFieldNames()),
                targetMetadata,
                new FieldList(outputFields));

        return node.discard(new FieldList(encodeAttributes));
    }
}
