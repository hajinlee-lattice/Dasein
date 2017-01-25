package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.CountryStandardizationFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.TypeConvertFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.StandardizationFlowParameter;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("sourceStandardizationFlow")
public class SourceStandardizationFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, StandardizationFlowParameter> {
    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(StandardizationFlowParameter parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        source = standardizeDomain(source, parameters.getDomainFields(), parameters.getAddOrReplaceDomainFields());
        source = standardizeCountry(source, parameters.getCountryFields(), parameters.getAddOrReplaceCountryFields(),
                parameters.getStandardCountries());
        source = stringToInt(source, parameters.getStringToIntFields(), parameters.getAddOrReplaceStringToIntFields());
        source = stringToLong(source, parameters.getStringToLongFields(),
                parameters.getAddOrReplaceStringToLongFields());
        source = filter(source, parameters.getFilterExpression(), parameters.getFilterFields());
        source = dedup(source, parameters.getDedupFields());
        source = addTimestamp(source, parameters.getUploadTimestampField());
        source = mark(source, parameters.getMarkerExpression(), parameters.getMarkerCheckFields(),
                parameters.getMarkerField());
        return source;
    }

    private Node mark(Node source, String markExpression, String[] markerCheckFields, String markerField) {
        if (StringUtils.isNotEmpty(markExpression)) {
            source = source.addFunction(markExpression + " ? true : false", new FieldList(markerCheckFields),
                    new FieldMetadata(markerField, Boolean.class));
        }
        return source;
    }

    private Node filter(Node source, String filterExpression, String[] filterFields) {
        if (StringUtils.isNotEmpty(filterExpression)) {
            source = source.filter(filterExpression, new FieldList(filterFields));
        }
        return source;
    }

    private Node dedup(Node source, String[] dedupFields) {
        if (dedupFields != null && dedupFields.length > 0) {
            source = source.groupByAndLimit(new FieldList(dedupFields), 1);
        }
        return source;
    }

    private Node standardizeDomain(Node source, String[] domainFields,
            StandardizationStrategy addOrReplaceDomainFields) {
        if (domainFields != null && domainFields.length > 0) {
            for (String domainField : domainFields) {
                source = source.apply(new DomainCleanupFunction(domainField, false), new FieldList(domainField),
                        new FieldMetadata(domainField, String.class));
            }
        }
        return source;
    }

    private Node standardizeCountry(Node source, String[] countryFields,
            StandardizationStrategy addOrReplaceCountryFields, Map<String, String> standardCountries) {
        if (countryFields != null && countryFields.length > 0) {
            for (String countryField : countryFields) {
                source = source.apply(new CountryStandardizationFunction(countryField, standardCountries),
                        new FieldList(countryField), new FieldMetadata(countryField, String.class));
            }
        }
        return source;
    }

    private Node stringToInt(Node source, String[] stringToIntFields,
            StandardizationStrategy addOrReplaceStringToIntFields) {
        if (stringToIntFields != null && stringToIntFields.length > 0) {
            for (String stringToIntField : stringToIntFields) {
                TypeConvertFunction function = new TypeConvertFunction(stringToIntField,
                        TypeConvertFunction.ConvertTrategy.STRING_TO_INT);
                source = source.apply(function, new FieldList(stringToIntField),
                        new FieldMetadata(stringToIntField, Integer.class));
            }
        }
        return source;
    }

    private Node stringToLong(Node source, String[] stringToLongFields,
            StandardizationStrategy addOrReplaceStringToLongFields) {
        if (stringToLongFields != null && stringToLongFields.length > 0) {
            for (String stringToLongField : stringToLongFields) {
                TypeConvertFunction function = new TypeConvertFunction(stringToLongField,
                        TypeConvertFunction.ConvertTrategy.STRING_TO_LONG);
                source = source.apply(function, new FieldList(stringToLongField),
                        new FieldMetadata(stringToLongField, Long.class));
            }
        }
        return source;
    }

    private Node addTimestamp(Node source, String uploadTimestampField) {
        if (StringUtils.isNotEmpty(uploadTimestampField)) {
            source = source.addTimestamp(uploadTimestampField);
        }
        return source;
    }

}
