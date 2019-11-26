package com.latticeengines.datacloud.dataflow.transformation.seed;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.FieldEnrichmentFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.SourceFieldEnrichmentTransformerConfig;

import cascading.tuple.Fields;

/**
 * A pipeline step in seeds rebuild pipelines, eg. OrbCacheSeed, HGCacheSeed,
 * RTSCacheSeed, etc
 */
@Component(SourceFieldEnchrimentFlow.DATAFLOW_BEAN)
public class SourceFieldEnchrimentFlow extends ConfigurableFlowBase<SourceFieldEnrichmentTransformerConfig> {

    public static final String DATAFLOW_BEAN = "sourceFieldEnrichmentFlow";
    public static final String TRANSFORMER = "sourceFieldEnrichmentTransformer";

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
        for (int i = 0; i < fromFields.size(); i++) {
            String fromField = fromFields.get(i);
            String toField = toFields.get(i);
            source = source.apply(new FieldEnrichmentFunction(fromField, toField), new FieldList(fromField, toField),
                    Arrays.asList(source.getSchema(fromField), source.getSchema(toField)),
                    new FieldList(origFieldNames), Fields.REPLACE);
        }
        return source;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SourceFieldEnrichmentTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER;

    }
}
