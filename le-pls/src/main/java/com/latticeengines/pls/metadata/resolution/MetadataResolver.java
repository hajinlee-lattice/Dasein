package com.latticeengines.pls.metadata.resolution;

import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;

public class MetadataResolver {
    private static Logger log = LoggerFactory.getLogger(MetadataResolver.class);
    private static List<String> ACCEPTED_BOOLEAN_VALUES = Arrays.asList("true", "false");
    private static final String USER_PREFIX = "user_";

    private String csvPath;
    private FieldMappingDocument fieldMappingDocument;
    private Configuration yarnConfiguration;
    private boolean cdlResolve = false;

    private static class Result {
        public List<FieldMapping> fieldMappings;
        public Table metadata;
    }

    private Result result;

    public MetadataResolver() {
        super();
    }

    public MetadataResolver(String csvPath, Configuration yarnConfiguration, FieldMappingDocument fieldMappingDocument,
            boolean cdlResolve) {
        this.csvPath = csvPath;
        this.yarnConfiguration = yarnConfiguration;
        this.fieldMappingDocument = fieldMappingDocument;
        this.result = new Result();
        this.cdlResolve = cdlResolve;
    }

    public MetadataResolver(String csvPath, Configuration yarnConfiguration,
            FieldMappingDocument fieldMappingDocument) {
        this.csvPath = csvPath;
        this.yarnConfiguration = yarnConfiguration;
        this.fieldMappingDocument = fieldMappingDocument;
        this.result = new Result();
    }

    public FieldMappingDocument getFieldMappingsDocumentBestEffort(Table metadata) {
        FieldMappingDocument fieldMappingsDocument = new FieldMappingDocument();

        result.metadata = metadata;
        result.fieldMappings = new ArrayList<>();
        if (cdlResolve) {
            calculateHelperCDL();
        } else {
            calculateHelper();
        }
        fieldMappingsDocument.setFieldMappings(result.fieldMappings);

        return fieldMappingsDocument;
    }

    public void calculateBasedOnFieldMappingDocumentAndTable(Table metadata) {
        result.metadata = metadata;
        calculateBasedOnMetadta();
        // sort the order based on header fields
        sortAttributesBasedOnSourceFileSequence(result.metadata);
    }

    public void sortAttributesBasedOnSourceFileSequence(Table table) {
        log.info("Current metadata attribute list: " + table.getAttributes());
        Set<String> headerFields = getHeaderFields();
        log.info("Current header list: " + headerFields);
        List<Attribute> attrs = headerFields.stream().map(h -> table.getAttributeFromDisplayName(h))
                .filter(Objects::nonNull).collect(Collectors.toList());
        List<Attribute> remaining = table.getAttributes().stream() //
                .filter(attr -> !attrs.contains(attr)) //
                .collect(Collectors.toList());
        attrs.addAll(remaining);
        table.setAttributes(attrs);
        log.info("After sorting header list: " + table.getAttributes());
    }

    public void calculateBasedOnFieldMappingDocument(Table metadata) {
        result.metadata = metadata;
        calculateBasedOnMetadta();
    }

    private void calculateBasedOnMetadta() {
        result.fieldMappings = new ArrayList<>();

        List<Attribute> attributes = result.metadata.getAttributes();
        Iterator<Attribute> attrIterator = attributes.iterator();
        while (attrIterator.hasNext()) {
            boolean foundMatchingAttribute = false;
            Attribute attribute = attrIterator.next();
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (fieldMapping.isMappedToLatticeField()) {
                    if (fieldMapping.getMappedField().equals(attribute.getName())) {
                        foundMatchingAttribute = true;
                        attribute.setDisplayName(fieldMapping.getUserField());
                        attribute.setPhysicalDataType(attribute.getPhysicalDataType().toLowerCase());
                        if (StringUtils.isNotEmpty(fieldMapping.getDateFormatString())) {
                            attribute.setDateFormatString(fieldMapping.getDateFormatString());
                        }
                        if (StringUtils.isNotEmpty(fieldMapping.getTimeFormatString())) {
                            attribute.setTimeFormatString(fieldMapping.getTimeFormatString());
                        }
                        if (StringUtils.isNotEmpty(fieldMapping.getTimezone())) {
                            attribute.setTimezone(fieldMapping.getTimezone());
                        }
                        break;
                    }
                }
            }
            if (!foundMatchingAttribute) {
                if (attribute.getDefaultValueStr() == null) {
                    attrIterator.remove();
                }
            }
        }

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (!fieldMapping.isMappedToLatticeField()) {
                attributes.add(getAttributeFromFieldName(fieldMapping));
            }
        }

        if (fieldMappingDocument.getIgnoredFields() != null) {
            attributes.removeIf(attr -> fieldMappingDocument.getIgnoredFields().contains(attr.getDisplayName()));
        }
        Attribute lastModified = result.metadata.getAttribute(InterfaceName.LastModifiedDate);
        if (lastModified == null) {
            result.metadata.setLastModifiedKey(null);
        }
        result.metadata.deduplicateAttributeNames();
    }

    public List<FieldMapping> calculateBasedOnExistingMetadata(Table metadataTable) {
        result.metadata = metadataTable;
        result.metadata.getExtracts().clear();
        result.fieldMappings = new ArrayList<>();
        calculateHelper();
        setUnmappedColumnsToCustomFieldsWithSameName();

        return result.fieldMappings;
    }

    public boolean isFieldMappingDocumentFullyDefined() {
        if (this.fieldMappingDocument == null) {
            return false;
        }

        List<FieldMapping> fieldMappings = fieldMappingDocument.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {
            if (fieldMapping.getMappedField() == null) {
                return false;
            }
        }
        return true;
    }

    public boolean isMetadataFullyDefined() {
        if (result == null) {
            return false;
        }

        for (FieldMapping fieldMapping : result.fieldMappings) {
            if (fieldMapping.getMappedField() == null) {
                return false;
            }
        }

        return true;
    }

    public Table getMetadata() {
        if (result == null) {
            return null;
        }
        if (!isMetadataFullyDefined()) {
            throw new RuntimeException("Metadata is not fully defined");
        }
        return result.metadata;
    }

    public void setFieldMappingDocument(FieldMappingDocument fieldMappingDocument) {
        this.fieldMappingDocument = fieldMappingDocument;
    }

    private void calculateHelperCDL() {
        Set<String> headerFields = getHeaderFields();
        List<Attribute> attributes = result.metadata.getAttributes();
        Iterator<Attribute> attrIterator = attributes.iterator();
        while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            if (headerFields.contains(attribute.getDisplayName())) {
                FieldMapping knownColumn = new FieldMapping();
                knownColumn.setUserField(attribute.getDisplayName());
                knownColumn.setMappedField(attribute.getName());
                knownColumn.setFieldType(getFieldTypeFromPhysicalType(attribute.getPhysicalDataType()));

                knownColumn.setMappedToLatticeField(true);
                if (StringUtils.isEmpty(attribute.getDateFormatString())
                        && StringUtils.isEmpty(attribute.getTimeFormatString())) {
                    if (UserDefinedType.DATE.equals(knownColumn.getFieldType())) {
                        List<String> columnFields = getColumnFieldsByHeader(knownColumn.getUserField());
                        MutablePair<String, String> result = distinguishDateAndTime(columnFields);
                        if (result != null) {
                            knownColumn
                                    .setDateFormatString(TimeStampConvertUtils.mapJavaToUserDateFormat(result.getLeft()));
                            knownColumn
                                    .setTimeFormatString(TimeStampConvertUtils.mapJavaToUserTimeFormat(result.getRight()));
                            knownColumn.setMappedToDateBefore(false);
                        }
                    }
                } else {
                    knownColumn.setDateFormatString(attribute.getDateFormatString());
                    knownColumn.setTimeFormatString(attribute.getTimeFormatString());
                    knownColumn.setMappedToDateBefore(true);
                }
                knownColumn.setTimezone(attribute.getTimezone());

                result.fieldMappings.add(knownColumn);
                headerFields.remove(attribute.getDisplayName());
            } else {
                Iterator<String> headerIterator = headerFields.iterator();
                FieldMapping knownColumn = new FieldMapping();
                while (headerIterator.hasNext()) {
                    String header = headerIterator.next();
                    if (isUserFieldMatchWithAttribute(header, attribute)) {
                        attribute.setDisplayName(header);
                        headerIterator.remove();
                        knownColumn.setUserField(header);

                        knownColumn.setMappedField(attribute.getName());
                        knownColumn.setFieldType(getFieldTypeFromPhysicalType(attribute.getPhysicalDataType()));
                        knownColumn.setMappedToLatticeField(true);
                        if (StringUtils.isEmpty(attribute.getDateFormatString())
                                && StringUtils.isEmpty(attribute.getTimeFormatString())) {
                            if (UserDefinedType.DATE.equals(knownColumn.getFieldType())) {
                                List<String> columnFields = getColumnFieldsByHeader(knownColumn.getUserField());
                                MutablePair<String, String> result = distinguishDateAndTime(columnFields);
                                if (result != null) {
                                    knownColumn.setDateFormatString(
                                            TimeStampConvertUtils.mapJavaToUserDateFormat(result.getLeft()));
                                    knownColumn.setTimeFormatString(
                                            TimeStampConvertUtils.mapJavaToUserTimeFormat(result.getRight()));
                                    knownColumn.setMappedToDateBefore(false);
                                }
                            }
                        } else {
                            knownColumn.setDateFormatString(attribute.getDateFormatString());
                            knownColumn.setTimeFormatString(attribute.getTimeFormatString());
                            knownColumn.setMappedToDateBefore(true);
                        }
                        knownColumn.setTimezone(attribute.getTimezone());
                        result.fieldMappings.add(knownColumn);
                        break;
                    }
                }
            }
        }

        for (final String headerField : headerFields) {
            FieldMapping unknownColumn = new FieldMapping();

            unknownColumn.setUserField(StringEscapeUtils.escapeHtml4(headerField));
            unknownColumn.setFieldType(getFieldTypeFromColumnContent(unknownColumn));
            //unknownColumn.setFieldType(getFieldTypeFromColumnContent(headerField));
            unknownColumn.setMappedToLatticeField(false);

            result.fieldMappings.add(unknownColumn);
        }

        Attribute lastModified = result.metadata.getAttribute(InterfaceName.LastModifiedDate);
        if (lastModified == null) {
            result.metadata.setLastModifiedKey(null);
        }
    }

    private void calculateHelper() {
        // Get header
        Set<String> headerFields = getHeaderFields();

        // Shed columns from metadata that are not in the uploaded file
        Set<String> missingRequiredFields = new HashSet<>();
        List<Attribute> attributes = result.metadata.getAttributes();
        Iterator<Attribute> attrIterator = attributes.iterator();

        while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            Iterator<String> headerIterator = headerFields.iterator();

            boolean foundMatchingAttribute = false;
            FieldMapping knownColumn = new FieldMapping();
            while (headerIterator.hasNext()) {
                String header = headerIterator.next();
                if (isUserFieldMatchWithAttribute(header, attribute)) {
                    foundMatchingAttribute = true;
                    attribute.setDisplayName(header);
                    headerIterator.remove();

                    knownColumn.setUserField(header);
                    knownColumn.setMappedField(attribute.getName());
                    knownColumn.setFieldType(getFieldTypeFromPhysicalType(attribute.getPhysicalDataType()));
                    if (UserDefinedType.DATE.equals(knownColumn.getFieldType())) {
                        List<String> columnFields = getColumnFieldsByHeader(knownColumn.getUserField());
                        MutablePair<String, String> result = distinguishDateAndTime(columnFields);
                        if (result != null) {
                            knownColumn.setDateFormatString(TimeStampConvertUtils.mapJavaToUserDateFormat(result.getLeft()));
                            knownColumn.setTimeFormatString(
                                    TimeStampConvertUtils.mapJavaToUserTimeFormat(result.getRight()));
                            knownColumn.setMappedToDateBefore(false);
                        }
                    }
                    knownColumn.setMappedToLatticeField(true);
                    result.fieldMappings.add(knownColumn);
                    break;
                }
            }
            if (!foundMatchingAttribute) {
                if (!attribute.isNullable()) {
                    missingRequiredFields.add(attribute.getName());
                }
                attrIterator.remove();
            }
        }

        // Add columns that are not in metadata to unknown columns
        for (final String headerField : headerFields) {
            FieldMapping unknownColumn = new FieldMapping();

            unknownColumn.setUserField(StringEscapeUtils.escapeHtml4(headerField));
            unknownColumn.setFieldType(getFieldTypeFromColumnContent(unknownColumn));
            unknownColumn.setMappedToLatticeField(false);

            result.fieldMappings.add(unknownColumn);
        }

        Attribute lastModified = result.metadata.getAttribute(InterfaceName.LastModifiedDate);
        if (lastModified == null) {
            result.metadata.setLastModifiedKey(null);
        }
    }

    private void setUnmappedColumnsToCustomFieldsWithSameName() {
        for (FieldMapping fieldMapping : result.fieldMappings) {
            if (!fieldMapping.isMappedToLatticeField()) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
            }
        }
    }

    public static UserDefinedType getFieldTypeFromPhysicalType(String attributeType) {
        UserDefinedType fieldType;
        switch (attributeType.toUpperCase()) {
        case "BOOLEAN":
            fieldType = UserDefinedType.BOOLEAN;
            break;
        case "LONG":
            fieldType = UserDefinedType.DATE;
            break;
        case "INT":
            fieldType = UserDefinedType.INTEGER;
            break;
        case "DOUBLE":
            fieldType = UserDefinedType.NUMBER;
            break;
        case "STRING":
        default:
            fieldType = UserDefinedType.TEXT;
            break;
        }
        return fieldType;
    }

    private boolean isUserFieldMatchWithAttribute(String header, Attribute attribute) {
        List<String> allowedDisplayNames = attribute.getAllowedDisplayNames();
        if (CollectionUtils.isNotEmpty(allowedDisplayNames)) {
            final String standardizedHeader = standardizeAttrName(header);
            String matchedDisplayName = allowedDisplayNames.stream() //
                    .filter(allowedName -> standardizeAttrName(allowedName).equalsIgnoreCase(standardizedHeader)) //
                    .findFirst().orElse(null);
            if (StringUtils.isNotBlank(matchedDisplayName)) {
                return true;
            }
        }
        return attribute.getDisplayName().equalsIgnoreCase(header);
    }

    private String standardizeAttrName(String attrName) {
        return attrName.replace("_", "").replace(" ", "").toUpperCase();
    }

    private Attribute getAttributeFromFieldName(FieldMapping fieldMapping) {
        Attribute attribute = new Attribute();

        String fieldType;
        if (fieldMapping.getFieldType() == null) {
            fieldType = getFieldTypeFromColumnContent(fieldMapping).getAvroType().toString()
                    .toLowerCase();
        } else {
            fieldType = fieldMapping.getFieldType().getAvroType().toString().toLowerCase();
        }
        if (cdlResolve) {
            String attrName =
                    ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(fieldMapping.getMappedField());
            if (!fieldMapping.isMappedToLatticeField() && !attrName.startsWith(USER_PREFIX)) {
                attrName = USER_PREFIX + attrName;
            }
            attribute.setName(attrName);
        } else {
            attribute
                    .setName(ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(fieldMapping.getUserField()));
        }
        attribute.setPhysicalDataType(fieldType);
        attribute.setDisplayName(fieldMapping.getUserField());
        attribute.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        attribute.setCategory(getCategoryBasedOnSchemaType(result.metadata.getInterpretation()));
        attribute.setFundamentalType(getFundamentalTypeFromFieldType(fieldType));
        attribute.setStatisticalType(getStatisticalTypeFromFieldType(fieldType));
        attribute.setNullable(true);
        attribute.setLogicalDataType(fieldMapping.getFieldType() == UserDefinedType.DATE ? LogicalDataType.Date
                : attribute.getLogicalDataType());
        attribute.setTags(ModelingMetadata.INTERNAL_TAG);
        attribute.setDateFormatString(fieldMapping.getDateFormatString());
        attribute.setTimeFormatString(fieldMapping.getTimeFormatString());
        attribute.setTimezone(fieldMapping.getTimezone());

        return attribute;
    }

    @VisibleForTesting
    String getCategoryBasedOnSchemaType(String schemaInterpretationString) {
        if (schemaInterpretationString == null) {
            log.warn("schema string is null");
            return ModelingMetadata.CATEGORY_LEAD_INFORMATION;
        }
        try {
            SchemaInterpretation schemaInterpretation = SchemaInterpretation.valueOf(schemaInterpretationString);
            switch (schemaInterpretation) {
            case Account:
            case SalesforceAccount:
                return ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION;
            case SalesforceLead:
            default:
                return ModelingMetadata.CATEGORY_LEAD_INFORMATION;
            }
        } catch (Exception e) {
            log.warn(String.format("Error finding proper category for schema string %s", schemaInterpretationString));
            return ModelingMetadata.CATEGORY_LEAD_INFORMATION;
        }
    }

    private String getFundamentalTypeFromFieldType(String fieldType) {
        String fundamentalType = null;
        switch (fieldType.toUpperCase()) {
        case "BOOLEAN":
            fundamentalType = ModelingMetadata.FT_BOOLEAN;
            break;
        case "DOUBLE":
            fundamentalType = ModelingMetadata.FT_NUMERIC;
            break;
        case "LONG":
            fundamentalType = ModelingMetadata.FT_YEAR;
            break;
        case "STRING":
        default:
            fundamentalType = ModelingMetadata.FT_ALPHA;
            break;
        }
        return fundamentalType;
    }

    private String getStatisticalTypeFromFieldType(String fieldType) {
        String statisticalType = null;
        switch (fieldType.toUpperCase()) {
        case "BOOLEAN":
            statisticalType = ModelingMetadata.NOMINAL_STAT_TYPE;
            break;
        case "DOUBLE":
            statisticalType = ModelingMetadata.RATIO_STAT_TYPE;
            break;
        case "STRING":
            statisticalType = ModelingMetadata.NOMINAL_STAT_TYPE;
            break;
        default:
            statisticalType = ModelingMetadata.RATIO_STAT_TYPE;
            break;
        }
        log.debug(String.format("The statistical type is %s", statisticalType));
        return statisticalType;
    }

    private List<String> getColumnFieldsByHeader(String columnHeaderName) {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        List<String> columnFields = null;
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            InputStream is = fs.open(new Path(csvPath));
            columnFields = ValidateFileHeaderUtils.getCSVColumnValues(columnHeaderName, is, closeableResourcePool);

            log.debug(String.format("column with header %s is: %s", columnHeaderName, columnFields.toString()));
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                throw new RuntimeException("Problem when closing the pool", e);
            }
        }
        return columnFields;
    }

    private UserDefinedType getFieldTypeFromColumnContent(FieldMapping fieldMapping) {
        String columnHeaderName = fieldMapping.getUserField();
        UserDefinedType fundamentalType = null;

        List<String> columnFields = getColumnFieldsByHeader(columnHeaderName);
        MutablePair<String, String> formatForDateAndTime = new MutablePair<>();
        if (columnFields.isEmpty()) {
            fundamentalType = UserDefinedType.TEXT;
        } else if (isBooleanTypeColumn(columnFields)) {
            fundamentalType = UserDefinedType.BOOLEAN;
        } else if (isIntegerTypeColumn(columnFields)) {
            fundamentalType = UserDefinedType.INTEGER;
        } else if (isDoubleTypeColumn(columnFields)) {
            fundamentalType = UserDefinedType.NUMBER;
        } else if (isDateTypeColumn(columnFields, formatForDateAndTime)) {
            fundamentalType = UserDefinedType.DATE;
            fieldMapping.setDateFormatString(formatForDateAndTime.getLeft());
            fieldMapping.setTimeFormatString(formatForDateAndTime.getRight());
            fieldMapping.setMappedToDateBefore(false);
        } else {
            fundamentalType = UserDefinedType.TEXT;
        }

        return fundamentalType;
    }

    @VisibleForTesting
    // Note that the returned data and time format are the user supported formats not the Java 8 formats.
    boolean isDateTypeColumn(List<String> columnFields, MutablePair<String, String> formatForDateAndTime) {
        MutablePair<String, String> result = distinguishDateAndTime(columnFields);
        if (result == null) {
            return false;
        } else {
            formatForDateAndTime.setLeft(TimeStampConvertUtils.mapJavaToUserDateFormat(result.getLeft()));
            formatForDateAndTime.setRight(TimeStampConvertUtils.mapJavaToUserTimeFormat(result.getRight()));
        }
        return true;
    }

    @VisibleForTesting
    MutablePair<String, String> distinguishDateAndTime(List<String> columnFields) {
        List<String> supportedDateTimeFormat = TimeStampConvertUtils.SUPPORTED_JAVA_DATE_TIME_FORMATS;
        Map<String, Integer> hitMap = new HashMap<String, Integer>();
        // iterate every value, generate number for supported format
        int nonConformingDate = 0;
        for (String columnField : columnFields) {
            if (StringUtils.isBlank(columnField)) {
                nonConformingDate++;
            } else {
                columnField = TimeStampConvertUtils.removeIso8601TandZFromDateTime(columnField);
                boolean isDate = false;
                for (String format : supportedDateTimeFormat) {
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
                    TemporalAccessor date = null;
                    try {
                        date = dtf.parse(columnField.trim());
                    } catch (DateTimeParseException e) {
                        log.debug("Found columnField unparsable as date/time: " + columnField);
                    }
                    if (date != null) {
                        hitMap.put(format, hitMap.containsKey(format) ? hitMap.get(format) + 1 : 1);
                        isDate = true;
                    }
                }
                if (!isDate) {
                    nonConformingDate++;
                }
            }
        }
        if (MapUtils.isEmpty(hitMap)) {
            return null;
        }
        // sort according to the occurrence times, then priority order defined
        // in supported data time list
        List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String, Integer>>(hitMap.entrySet());
        Collections.sort(entries,
                (entry1, entry2) -> entry1.getValue().equals(entry2.getValue())
                        ? Integer.compare(supportedDateTimeFormat.indexOf(entry1.getKey()),
                                supportedDateTimeFormat.indexOf(entry2.getKey()))
                        : entry2.getValue().compareTo(entry1.getValue()));
        String expectedFormat = entries.get(0).getKey();
        int mostCommonDate = entries.get(0).getValue();
        if (nonConformingDate > mostCommonDate) {
            return null;
        }

        // legal date time formats are delimited by space defined in
        // TimeStampConvertUtils
        int index = expectedFormat.indexOf(" ");
        if (index == -1) {
            return new MutablePair<String, String>(expectedFormat, null);
        } else {
            return new MutablePair<String, String>(expectedFormat.substring(0, index),
                    expectedFormat.substring(index + 1));
        }
    }

    @VisibleForTesting
    boolean isBooleanTypeColumn(List<String> columnFields) {
        for (String columnField : columnFields) {
            if (StringUtils.isNotBlank(columnField)
                    && !ACCEPTED_BOOLEAN_VALUES.contains(columnField.toLowerCase())) {
                return false;
            }
        }
        return true;
    }

    private boolean isDoubleTypeColumn(List<String> columnFields) {
        for (String columnField : columnFields) {
            if (StringUtils.isNotBlank(columnField)) {
                try {
                    Double.parseDouble(columnField);
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean isIntegerTypeColumn(List<String> columnFields) {
        for (String columnField : columnFields) {
            if (StringUtils.isNotBlank(columnField)) {
                try {
                    Integer.parseInt(columnField);
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        }

        return true;
    }

    private Set<String> getHeaderFields() {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            InputStream is = fs.open(new Path(csvPath));
            return ValidateFileHeaderUtils.getCSVHeaderFields(is, closeableResourcePool);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                throw new RuntimeException("Problem when closing the pool", e);
            }
        }
    }
}
