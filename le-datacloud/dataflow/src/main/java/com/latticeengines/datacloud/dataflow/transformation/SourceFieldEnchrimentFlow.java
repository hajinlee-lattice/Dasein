package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.FieldEnrichmentFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SourceFieldEnrichmentTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("sourceFieldEnrichmentFlow")
public class SourceFieldEnchrimentFlow extends ConfigurableFlowBase<SourceFieldEnrichmentTransformerConfig> {
    private static final Log log = LogFactory.getLog(SourceFieldEnchrimentFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SourceFieldEnrichmentTransformerConfig config = getTransformerConfig(parameters);
        List<String> fromFields = config.getFromFields();
        List<String> toFields = config.getToFields();
        if (CollectionUtils.isEmpty(fromFields) || CollectionUtils.isEmpty(toFields)
                || fromFields.size() != toFields.size()) {
            throw new RuntimeException("Sizes of fromFields and toFields does not match!");
        }
        Node source = addSource(parameters.getBaseTables().get(0));
        List<String> origFieldNames = source.getFieldNames();
        List<String> newFieldNames = resolveFieldNames(origFieldNames);
        for (int i = 0; i < fromFields.size(); i++) {
            String fromField = fromFields.get(i);
            String toField = toFields.get(i);
            source = source.apply(new FieldEnrichmentFunction(fromField, toField), new FieldList(fromField, toField),
                    Arrays.asList(source.getSchema(fromField), source.getSchema(toField)),
                    new FieldList(newFieldNames), Fields.REPLACE);
        }
        if (!config.isDebug()) {
            source = source.retain(new FieldList(newFieldNames));
        }

        return source;
    }

    private List<String> resolveFieldNames(List<String> origFieldNames) {
        List<String> newFieldNames = new ArrayList<>();
        for (String origFieldName : origFieldNames) {
            String fieldName = origFieldName.trim().toLowerCase();
            if (fieldName.startsWith("__") || fieldName.equalsIgnoreCase("LatticeAccountId")) {
                continue;
            }
            newFieldNames.add(origFieldName);
        }

        return newFieldNames;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SourceFieldEnrichmentTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "sourceFieldEnrichmentFlow";
    }

    @Override
    public String getTransformerName() {
        return "sourceFieldEnrichmentTransformer";

    }
}