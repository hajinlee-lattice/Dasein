package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.BucketedFilter.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETED_FILTER;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.dataflow.transformation.FilterBucketed;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.FilterBucketedParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BucketedFilterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(TRANSFORMER_NAME)
public class BucketedFilter extends AbstractDataflowTransformer<BucketedFilterConfig, FilterBucketedParameters> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_BUCKETED_FILTER;

    private static final Log log = LogFactory.getLog(BucketedFilter.class);

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public String getDataFlowBeanName() {
        return FilterBucketed.BEAN_NAME;
    }

    @Override
    protected boolean validateConfig(BucketedFilterConfig config, List<String> sourceNames) {
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return BucketedFilterConfig.class;
    }

    @Override
    protected Class<FilterBucketedParameters> getDataFlowParametersClass() {
        return FilterBucketedParameters.class;
    }

    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, FilterBucketedParameters parameters,
                                         BucketedFilterConfig configuration) {
        parameters.originalAttrs = configuration.getOriginalAttrs();
        parameters.encAttrPrefix = configuration.getEncAttrPrefix();
    }

    @Override
    protected Schema getTargetSchema(Table result, FilterBucketedParameters parameters, List<Schema> baseAvscSchemas) {
        if (baseAvscSchemas != null) {
            Schema schema = baseAvscSchemas.get(0);
            if (schema != null) {
                log.info("Found schema from base sources' avsc. Modifying it.");
                return modifyBaseSchema(schema, parameters);
            }
        }
        return null;
    }

    private Schema modifyBaseSchema(Schema baseSchema, FilterBucketedParameters parameters) {
        Set<String> originalFieldSet = new HashSet<>(parameters.originalAttrs);
        String encAttrPrefix = StringUtils.isBlank(parameters.encAttrPrefix) ? CEAttr : parameters.encAttrPrefix;
        List<String> toRemove = baseSchema.getFields().stream() //
                .map(Schema.Field::toString) //
                .filter(f -> !originalFieldSet.contains(f) && !f.startsWith(encAttrPrefix)) // not in original, and not CEAttr
                .collect(Collectors.toList());
        log.info("Removing these fields: " + StringUtils.join(toRemove, ", "));
        return AvroUtils.removeFields(baseSchema, toRemove.toArray(new String[toRemove.size()]));
    }

}
