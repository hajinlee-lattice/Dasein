package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.FilterBucketedParameters;

/**
 * Directly copy/merge base sources into output avros
 */
@Component(FilterBucketed.BEAN_NAME)
public class FilterBucketed extends TypesafeDataFlowBuilder<FilterBucketedParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(FilterBucketed.class);

    public static final String BEAN_NAME = "filterBucketed";

    @Override
    public Node construct(FilterBucketedParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));

        List<String> sourceFieldNames = source.getFieldNames();
        Set<String> originalFields = new HashSet<>(parameters.originalAttrs);
        String encAttrPrefix = StringUtils.isBlank(parameters.encAttrPrefix) ? CEAttr : parameters.encAttrPrefix;
        sourceFieldNames.removeIf(f -> !originalFields.contains(f) && !f.startsWith(encAttrPrefix));

        return source.retain(new FieldList(sourceFieldNames));
    }

}
