package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AddNotNullFieldFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DenseFieldsCountFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceDedupeWithDenseFieldsTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("sourceDedupeWithDenseFieldsFlow")
public class SourceDedupeWithDenseFieldsFlow extends ConfigurableFlowBase<SourceDedupeWithDenseFieldsTransformerConfig> {
    private static final String DENSE_FIELDS_COUNT = "__DENSE_FIELDS_COUNT__";
    private static final String NEW_DEDUPE_FIELD_PREFIX = "__NEW_DEDUPE_FIELD_PREFIX__";

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SourceDedupeWithDenseFieldsTransformerConfig config = getTransformerConfig(parameters);

        Node source = addSource(parameters.getBaseTables().get(0));
        List<String> origFieldNames = source.getFieldNames();

        List<String> dedupeFields = config.getDedupeFields();
        List<String> denseFields = config.getDenseFields();
        List<String> sortFields = config.getSortFields();
        if (CollectionUtils.isEmpty(denseFields)) {
            throw new RuntimeException("Missing required fields for dedupe!");
        }
        if (CollectionUtils.isEmpty(sortFields)) {
            sortFields = new ArrayList<>();
            sortFields.add(DENSE_FIELDS_COUNT);
        } else {
            sortFields.add(DENSE_FIELDS_COUNT);
        }
        source = source.apply(new DenseFieldsCountFunction(denseFields, DENSE_FIELDS_COUNT),
                new FieldList(denseFields), new FieldMetadata(DENSE_FIELDS_COUNT, Integer.class));

        List<String> newDedupeFieldNames = new ArrayList<>();
        for (String dedupeField : dedupeFields) {
            String newDedupeField = NEW_DEDUPE_FIELD_PREFIX + dedupeField;
            source = source.apply(new AddNotNullFieldFunction(dedupeField, newDedupeField), new FieldList(dedupeField),
                    new FieldMetadata(newDedupeField, String.class));
            newDedupeFieldNames.add(newDedupeField);
        }

        source = source.groupByAndLimit(new FieldList(newDedupeFieldNames), new FieldList(sortFields), 1, true,
                true);
        source = source.retain(new FieldList(origFieldNames));

        return source;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SourceDedupeWithDenseFieldsTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "sourceDedupeWithDenseFieldsFlow";
    }

    @Override
    public String getTransformerName() {
        return "sourceDedupeWithDenseFieldsTransformer";

    }
}
