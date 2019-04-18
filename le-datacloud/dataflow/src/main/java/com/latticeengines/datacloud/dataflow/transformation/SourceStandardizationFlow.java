package com.latticeengines.datacloud.dataflow.transformation;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.AddMD5Hash;
import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateIndustryFromNaicsFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.CountryStandardizationFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DunsCleanupFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.StateStandardizationFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.StringStandardizationFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.TypeConvertFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ValueToRangeMappingFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.StandardizationFlowParameter;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateRangeStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.IDStrategy;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(SourceStandardizationFlow.DATAFLOW_BEAN_NAME)
public class SourceStandardizationFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, StandardizationFlowParameter> {

    private static final String IS_VALID_DOMAIN = "IsValidDomain";
    private static final String DOMAIN = "Domain";
    public static final String TRANSFORMER_NAME = "standardizationTransformer";
    public static final String DATAFLOW_BEAN_NAME = "sourceStandardizationFlow";

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(StandardizationFlowParameter parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        for (StandardizationTransformerConfig.StandardizationStrategy strategy : parameters.getSequence()) {
            switch (strategy) {
            case DOMAIN:
                source = standardizeDomain(source, parameters.getDomainFields());
                break;
            case DUNS:
                source = standardizeDuns(source, parameters.getDunsFields());
                break;
            case COUNTRY:
                source = standardizeCountry(source, parameters.getCountryFields(), parameters.getStandardCountries());
                break;
            case CONVERT_TYPE:
                source = convertType(source, parameters.getConvertTypeFields(), parameters.getConvertTypeStrategies());
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
            case ADD_FIELD:
                source = addFields(source, parameters.getAddFields(), parameters.getAddFieldValues(),
                        parameters.getAddFieldTypes());
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
            case CONSOLIDATE_RANGE:
                source = consolidateRange(source, parameters.getAddConsolidatedRangeFields(),
                        parameters.getConsolidateRangeStrategies(), parameters.getRangeInputFields(),
                        parameters.getRangeMapFileNames());
                break;
            case STRING:
                source = standardizeString(source, parameters.getStringFields());
                break;
            case STATE:
                source = standardizeState(source, parameters.getCountryFields(), parameters.getStateFields());
                break;
            case DISCARD:
                source = discard(source, parameters.getDiscardFields());
                break;
            case ADD_ID:
                source = addId(source, parameters.getIdFields(), parameters.getIdStrategies());
                break;
            case COPY:
                source = copy(source, parameters.getCopyFields());
                break;
            case CHECKSUM:
                source = checksum(source, parameters.getChecksumExcludeFields(), parameters.getChecksumField());
                break;
            case UPDATE:
                source = update(source, parameters.getUpdateFields(), parameters.getUpdateExpressions(),
                        parameters.getUpdateInputFields());
                break;
            case SAMPLE:
                source = sample(source, parameters.getSampleFraction());
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Standardization strategy %s is not supported", strategy.name()));
            }
        }

        return source;
    }

    private Node sample(Node source, Float fraction) {
        return source.sample(fraction);
    }

    private Node update(Node source, String[] updateFields, String[] updateExpressions, String[][] updateInputFields) {
        for (int i = 0; i < updateFields.length; i++) {
            FieldMetadata fm = new FieldMetadata("_UPDATE_NEW_" + updateFields[i],
                    source.getSchema(updateFields[i]).getJavaType());
            source = source.apply(updateExpressions[i], new FieldList(updateInputFields[i]), fm)
                    .discard(new FieldList(updateFields[i]))
                    .rename(new FieldList("_UPDATE_NEW_" + updateFields[i]), new FieldList(updateFields[i]));
            source = source.retain(new FieldList(source.getFieldNames()));
        }
        return source;
    }

    private Node checksum(Node source, String[] excludeFields, String checksumField) {
        Set<String> toExclude = excludeFields == null ? new HashSet<>() : new HashSet<>(Arrays.asList(excludeFields));
        source = source
                .apply(new AddMD5Hash(new Fields(checksumField), toExclude), new FieldList(source.getFieldNames()),
                        new FieldMetadata(checksumField, String.class));
        return source;
    }

    private Node copy(Node source, String[][] copyFields) {
        List<FieldMetadata> fms = source.getSchema();
        for (String[] couple : copyFields) {    // couple[0]: fromField; couple[1]: toField
            FieldMetadata newFM = null;
            for (FieldMetadata fm : fms) {
                if (fm.getFieldName().equals(couple[0])) {
                    newFM = new FieldMetadata(couple[1], fm.getJavaType());
                }
            }
            source = source.apply(couple[0], new FieldList(couple[0]), newFM);
        }
        return source;
    }

    private Node addId(Node source, String[] idFields, IDStrategy[] idStrategies) {
        for (int i = 0; i < idFields.length; i++) {
            if (source.getSchema(idFields[i]) != null) {
                source = source.discard(idFields[i]);
            }
            switch (idStrategies[i]) {
            case ROWID:
                source = source.addRowID(new FieldMetadata(idFields[i], Long.class));
                break;
            case UUID:
                source = source.addUUID(idFields[i]);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("ID strategy %s is not supported", idStrategies[i].name()));
            }
        }
        return source;
    }

    private Node discard(Node source, String[] discardFields) {
        if (discardFields != null && discardFields.length != 0) {
            source = source.discard(new FieldList(discardFields));
        }
        return source;
    }

    private Node standardizeState(Node source, String[] countryFields, String[] stateFields) {
        if (countryFields != null && countryFields.length > 0 && stateFields != null && stateFields.length > 0) {
            String countryField = countryFields[0];
            for (String stateField : stateFields) {
                source = source.apply(
                        new StateStandardizationFunction(
                                new Fields(source.getFieldNames().toArray(new String[source.getFieldNames().size()])),
                                countryField, stateField),
                        new FieldList(source.getFieldNames()), source.getSchema(),
                        new FieldList(source.getFieldNames()), Fields.REPLACE);
            }
        }
        return source;
    }

    private Node standardizeString(Node source, String[] stringFields) {
        if (stringFields != null && stringFields.length > 0) {
            for (String stringField : stringFields) {
                source = source.apply(new StringStandardizationFunction(stringField), new FieldList(stringField),
                        new FieldMetadata(stringField, String.class));
            }
        }
        return source;
    }

    private Node consolidateRange(Node source, String[] addConsolidatedRangeFields,
            ConsolidateRangeStrategy[] strategies, String[] rangeInputFields, String[] rangeMapFileNames) {
        if (addConsolidatedRangeFields != null && strategies != null && rangeInputFields != null && rangeMapFileNames != null
                && addConsolidatedRangeFields.length == strategies.length && strategies.length == rangeInputFields.length
                && rangeInputFields.length == rangeMapFileNames.length) {
            for (int i = 0; i < addConsolidatedRangeFields.length; i++) {
                String addConsolidatedRangeField = addConsolidatedRangeFields[i];
                ConsolidateRangeStrategy strategy = strategies[i];
                String rangeInputField = rangeInputFields[i];
                String fileName = rangeMapFileNames[i];
                switch (strategy) {
                case MAP_RANGE:
                    Map<Serializable, Serializable> rangeMap = new HashMap<>();
                    InputStream is = Thread.currentThread().getContextClassLoader()
                            .getResourceAsStream("etl/" + fileName);
                    if (is == null) {
                        throw new RuntimeException("Cannot find resource " + fileName);
                    }
                    Scanner scanner = new Scanner(is);
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        String[] pair = line.split("\\|");
                        rangeMap.put(pair[0], pair[1]);
                    }
                    scanner.close();
                    source = source.apply(new MappingFunction(rangeInputField, addConsolidatedRangeField, rangeMap),
                            new FieldList(rangeInputField), new FieldMetadata(addConsolidatedRangeField, String.class));
                    break;
                case MAP_VALUE:
                    List<String[]> rangeValueMap = new ArrayList<>();
                    is = Thread.currentThread().getContextClassLoader().getResourceAsStream("etl/" + fileName);
                    if (is == null) {
                        throw new RuntimeException("Cannot find resource " + fileName);
                    }
                    scanner = new Scanner(is);
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        String[] pair = line.split("\\|");
                        rangeValueMap.add(pair);
                    }
                    scanner.close();
                    source = source.apply(
                            new ValueToRangeMappingFunction(rangeInputField, addConsolidatedRangeField, rangeValueMap),
                            new FieldList(rangeInputField), new FieldMetadata(addConsolidatedRangeField, String.class));
                    break;
                default:
                    throw new RuntimeException(
                            String.format("ConsolidateRangeStrategy %s is not supported", strategy.name()));
                }
            }
        }
        return source;
    }

    private Node consolidateIndustry(Node source, String addConsolidatedIndustryField,
            StandardizationTransformerConfig.ConsolidateIndustryStrategy strategy, String industryField,
            String industryMapFileName, String naicsField, String naicsMapFileName) {
        if (StringUtils.isNotEmpty(addConsolidatedIndustryField) && strategy != null
                && (StringUtils.isNotEmpty(industryField) && StringUtils.isNotEmpty(industryMapFileName))
                || (StringUtils.isNotEmpty(naicsField) && StringUtils.isNotEmpty(naicsMapFileName))) {
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
            source = source.retain(new FieldList(source.getFieldNames()));
        }
        return source;
    }

    private Node renameAllFields(Node node) {
        List<String> newNames = new ArrayList<String>();
        List<String> oldNames = node.getFieldNames();
        for (String oldName : oldNames) {
            newNames.add(node.getPipeName() + "_" + oldName);
        }
        node = node.rename(new FieldList(oldNames), new FieldList(newNames));
        return node.retain(new FieldList(node.getFieldNames()));
    }

    private String getRenamedFieldName(Node node, String originalName) {
        return node.getPipeName() + "_" + originalName;
    }

    private Node addFields(Node source, String[] addFields, Object[] addFieldValues,
            StandardizationTransformerConfig.FieldType[] addFieldTypes) {
        if (addFields != null && addFields.length > 0) {
            for (int i = 0; i < addFields.length; i++) {
                switch (addFieldTypes[i]) {
                case STRING:
                    source = source.addColumnWithFixedValue(addFields[i], addFieldValues[i], String.class);
                    break;
                case INT:
                    source = source.addColumnWithFixedValue(addFields[i], addFieldValues[i], Integer.class);
                    break;
                case LONG:
                    source = source.addColumnWithFixedValue(addFields[i], addFieldValues[i], Long.class);
                    break;
                case BOOLEAN:
                    source = source.addColumnWithFixedValue(addFields[i], addFieldValues[i], Boolean.class);
                    break;
                case FLOAT:
                    source = source.addColumnWithFixedValue(addFields[i], addFieldValues[i], Float.class);
                    break;
                case DOUBLE:
                    source = source.addColumnWithFixedValue(addFields[i], addFieldValues[i], Double.class);
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
                source = source.retain(new FieldList(source.getFieldNames()));
            }
        }
        return source;
    }

    private Node mark(Node source, String markExpression, String[] markerCheckFields, String markerField) {
        if (StringUtils.isNotEmpty(markExpression)) {
            source = source.apply(markExpression + " ? true : false", new FieldList(markerCheckFields),
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

    private Node standardizeDuns(Node source, String[] dunsFields) {
        if (dunsFields != null && dunsFields.length > 0) {
            for (String dunsField : dunsFields) {
                source = source.apply(new DunsCleanupFunction(dunsField), new FieldList(dunsField),
                        new FieldMetadata(dunsField, String.class));
            }
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

    private Node convertType(Node source, String[] convertTypeFields, TypeConvertStrategy[] strategies) {
        if (convertTypeFields != null && convertTypeFields.length > 0 && strategies != null
                && strategies.length > 0 && convertTypeFields.length == strategies.length) {
            for (int i = 0; i < convertTypeFields.length; i++) {
                String convertTypeField = convertTypeFields[i];
                TypeConvertStrategy strategy = strategies[i];
                TypeConvertFunction function = new TypeConvertFunction(convertTypeField, strategy, false);
                switch (strategy) {
                    case ANY_TO_STRING:
                        source = source.apply(function, new FieldList(convertTypeField),
                                new FieldMetadata(convertTypeField, String.class));
                        break;
                    case STRING_TO_INT:
                        source = source.apply(function, new FieldList(convertTypeField),
                                new FieldMetadata(convertTypeField, Integer.class));
                        break;
                    case STRING_TO_LONG:
                        source = source.apply(function, new FieldList(convertTypeField),
                                new FieldMetadata(convertTypeField, Long.class));
                        break;
                    case STRING_TO_BOOLEAN:
                        source = source.apply(function, new FieldList(convertTypeField),
                                new FieldMetadata(convertTypeField, Boolean.class));
                        break;
                    case ANY_TO_INT:
                        source = source.apply(function, new FieldList(convertTypeField),
                                new FieldMetadata(convertTypeField, Integer.class));
                        break;
                    case ANY_TO_DOUBLE:
                        source = source.apply(function, new FieldList(convertTypeField),
                                new FieldMetadata(convertTypeField, Double.class));
                        break;
                    case ANY_TO_LONG:
                        source = source.apply(function, new FieldList(convertTypeField),
                                new FieldMetadata(convertTypeField, Long.class));
                        break;
                    case ANY_TO_BOOLEAN:
                        source = source.apply(function, new FieldList(convertTypeField),
                                new FieldMetadata(convertTypeField, Boolean.class));
                        break;
                    default:
                        break;
                }
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
