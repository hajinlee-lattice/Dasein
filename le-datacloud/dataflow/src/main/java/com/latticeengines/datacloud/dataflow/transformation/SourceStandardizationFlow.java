package com.latticeengines.datacloud.dataflow.transformation;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.ConsolidateIndustryFromNaicsFunction;
import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.CountryStandardizationFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.TypeConvertFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.StandardizationFlowParameter;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("sourceStandardizationFlow")
public class SourceStandardizationFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, StandardizationFlowParameter> {
    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    private final static String IS_VALID_DOMAIN = "IsValidDomain";
    private final static String DOMAIN = "Domain";

    @Override
    public Node construct(StandardizationFlowParameter parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        for (StandardizationTransformerConfig.StandardizationStrategy strategy : parameters.getSequence()) {
            switch (strategy) {
            case DOMAIN:
                source = standardizeDomain(source, parameters.getDomainFields());
                break;
            case COUNTRY:
                source = standardizeCountry(source, parameters.getCountryFields(), parameters.getStandardCountries());
                break;
            case STRING_TO_INT:
                source = stringToInt(source, parameters.getStringToIntFields());
                break;
            case STRING_TO_LONG:
                source = stringToLong(source, parameters.getStringToLongFields());
                break;
            case DEDUP:
                source = dedup(source, parameters.getDedupFields());
                break;
            case FILTER:
                source = filter(source, parameters.getFilterExpression(), parameters.getFilterFields());
                break;
            case UPLOAD_TIMESTAMP:
                source = addTimestamp(source, parameters.getUploadTimestampField());
                break;
            case MARKER:
                source = mark(source, parameters.getMarkerExpression(), parameters.getMarkerCheckFields(),
                        parameters.getMarkerField());
                break;
            case RENAME:
                source = rename(source, parameters.getRenameFields());
                break;
            case RETAIN:
                source = retain(source, parameters.getRetainFields());
                break;
            case ADD_NULL_FIELD:
                source = addNullField(source, parameters.getAddNullFields(), parameters.getAddNullFieldTypes());
                break;
            case VALID_DOMAIN:
                Node domainValidation = addSource(parameters.getBaseTables().get(1));
                source = validateDomain(source, domainValidation, parameters.getIsValidDomainField(),
                        parameters.getValidDomainCheckField());
                break;
            case CONSOLIDATE_INDUSTRY:
                source = consolidateIndustry(source, parameters.getAddConsolidatedIndustryField(),
                        parameters.getConsolidateIndustryStrategy(), parameters.getIndustryField(),
                        parameters.getIndustryMapFileName(), parameters.getNaicsField(),
                        parameters.getNaicsMapFileName());
                break;
            default:
                break;
            }
        }

        return source;
    }
    
    private Node consolidateIndustry(Node source, String addConsolidatedIndustryField,
            StandardizationTransformerConfig.ConsolidateIndustryStrategy strategy, String industryField,
            String industryMapFileName, String naicsField, String naicsMapFileName) {
        switch (strategy) {
        case MAP_INDUSTRY:
            Map<Serializable, Serializable> industryMap = new HashMap<>();
            InputStream is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("etl/" + industryMapFileName);
            if (is == null) {
                throw new RuntimeException("Cannot find resource " + industryMapFileName);
            }
            Scanner scanner = new Scanner(is);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] pair = line.split("\\|");
                industryMap.put(pair[0], pair[1]);
            }
            scanner.close();
            source = source.apply(new MappingFunction(industryField, addConsolidatedIndustryField, industryMap),
                    new FieldList(industryField), new FieldMetadata(addConsolidatedIndustryField, String.class));
            break;
        case PARSE_NAICS:
            Map<Integer, Map<Serializable, Serializable>> naicsMap = new TreeMap<>(Collections.reverseOrder());
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream("etl/" + naicsMapFileName);
            if (is == null) {
                throw new RuntimeException("Cannot find resource " + naicsMapFileName);
            }
            scanner = new Scanner(is);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] pair = line.split("\\|");
                if (!naicsMap.containsKey(pair[0].length())) {
                    naicsMap.put(pair[0].length(), new HashMap<>());
                }
                naicsMap.get(pair[0].length()).put(pair[0], pair[1]);
            }
            scanner.close();
            source = source.apply(
                    new ConsolidateIndustryFromNaicsFunction(naicsField, addConsolidatedIndustryField, naicsMap),
                    new FieldList(naicsField), new FieldMetadata(addConsolidatedIndustryField, String.class));
            break;
        default:
            throw new RuntimeException(
                    String.format("ConsolidateIndustryStrategy %s is not supported", strategy.name()));
        }
        return source;
    }

    private Node validateDomain(Node source, Node domainValidation, String isValidDomainField,
            String validDomainCheckField) {
        if (StringUtils.isNotEmpty(isValidDomainField) && StringUtils.isNotEmpty(validDomainCheckField)) {
            domainValidation = renameAllFields(domainValidation);
            Node joined = source.join(new FieldList(validDomainCheckField), domainValidation,
                    new FieldList(getRenamedFieldName(domainValidation, DOMAIN)), JoinType.LEFT);
            List<String> retainedFields = new ArrayList<String>();
            retainedFields.addAll(source.getFieldNames());
            retainedFields.add(getRenamedFieldName(domainValidation, IS_VALID_DOMAIN));
            source = joined.retain(new FieldList(retainedFields));
            source = source.rename(new FieldList(getRenamedFieldName(domainValidation, IS_VALID_DOMAIN)),
                    new FieldList(isValidDomainField));
        }
        return source;
    }

    private Node renameAllFields(Node node) {
        List<String> newNames = new ArrayList<String>();
        List<String> oldNames = node.getFieldNames();
        for (String oldName : oldNames) {
            newNames.add(node.getPipeName() + "_" + oldName);
        }
        return node.rename(new FieldList(oldNames), new FieldList(newNames));
    }

    private String getRenamedFieldName(Node node, String originalName) {
        return node.getPipeName() + "_" + originalName;
    }

    private Node addNullField(Node source, String[] addNullFields,
            StandardizationTransformerConfig.FieldType[] addNullFieldTypes) {
        if (addNullFields != null && addNullFields.length > 0) {
            for (int i = 0; i < addNullFields.length; i++) {
                switch (addNullFieldTypes[i]) {
                case STRING:
                    source = source.addColumnWithFixedValue(addNullFields[i], null, String.class);
                    break;
                case INT:
                    source = source.addColumnWithFixedValue(addNullFields[i], null, Integer.class);
                    break;
                case LONG:
                    source = source.addColumnWithFixedValue(addNullFields[i], null, Long.class);
                    break;
                case BOOLEAN:
                    source = source.addColumnWithFixedValue(addNullFields[i], null, Boolean.class);
                    break;
                case FLOAT:
                    source = source.addColumnWithFixedValue(addNullFields[i], null, Float.class);
                    break;
                case DOUBLE:
                    source = source.addColumnWithFixedValue(addNullFields[i], null, Double.class);
                    break;
                default:
                    break;
                }

            }
        }
        return source;
    }

    private Node retain(Node source, String[] retainFields) {
        if (retainFields != null && retainFields.length > 0) {
            source = source.retain(new FieldList(retainFields));
        }
        return source;
    }

    private Node rename(Node source, String[][] renameFields) {
        if (renameFields != null && renameFields.length > 0) {
            for (String[] renameFieldMap : renameFields) {
                source = source.rename(new FieldList(renameFieldMap[0]), new FieldList(renameFieldMap[1]));
            }
        }
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

    private Node standardizeDomain(Node source, String[] domainFields) {
        if (domainFields != null && domainFields.length > 0) {
            for (String domainField : domainFields) {
                source = source.apply(new DomainCleanupFunction(domainField, false), new FieldList(domainField),
                        new FieldMetadata(domainField, String.class));
            }
        }
        return source;
    }

    private Node standardizeCountry(Node source, String[] countryFields, Map<String, String> standardCountries) {
        if (countryFields != null && countryFields.length > 0) {
            for (String countryField : countryFields) {
                source = source.apply(new CountryStandardizationFunction(countryField, standardCountries),
                        new FieldList(countryField), new FieldMetadata(countryField, String.class));
            }
        }
        return source;
    }

    private Node stringToInt(Node source, String[] stringToIntFields) {
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

    private Node stringToLong(Node source, String[] stringToLongFields) {
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
