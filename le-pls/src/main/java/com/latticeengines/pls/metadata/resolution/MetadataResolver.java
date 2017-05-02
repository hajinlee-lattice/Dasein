package com.latticeengines.pls.metadata.resolution;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
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
    private static Logger log = Logger.getLogger(MetadataResolver.class);
    private static List<String> ACCEPTED_BOOLEAN_VALUES = Arrays.asList("true", "false", "1", "0");

    private static final Set<String> BOOLEAN_SET = Sets.newHashSet(new String[] { "Interest_esb__c", "Interest_tcat__c",
            "kickboxAcceptAll", "Free_Email_Address__c", "kickboxFree", "Unsubscribed", "kickboxDisposable",
            "HasAnypointLogin", "HasCEDownload", "HasEEDownload" });
    private static final Set<String> STR_SET = Sets.newHashSet(
            new String[] { "Lead_Source_Asset__c", "kickboxStatus", "SICCode", "Source_Detail__c", "Cloud_Plan__c" });

    private String csvPath;
    private FieldMappingDocument fieldMappingDocument;
    private Configuration yarnConfiguration;

    private static class Result {
        public List<FieldMapping> fieldMappings;
        public Table metadata;
    }

    private Result result;

    public MetadataResolver() {
        super();
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
        calculateHelper();
        fieldMappingsDocument.setFieldMappings(result.fieldMappings);

        return fieldMappingsDocument;
    }

    public void calculateBasedOnFieldMappingDocumentAndTable(Table metadata) {
        result.metadata = metadata;
        calculateBasedOnMetadta();
        // sort the order based on header fields
        sortAttributesBasedOnSourceFileSequence();
    }

    private void sortAttributesBasedOnSourceFileSequence() {
        log.info("Current metadata attribute list: " + result.metadata.getAttributes());
        Set<String> headerFields = getHeaderFields();
        log.info("Current header list: " + headerFields);
        List<Attribute> attrs = new ArrayList<>();
        for (final String header : headerFields) {
            Attribute attr = Iterables.find(result.metadata.getAttributes(), new Predicate<Attribute>() {
                @Override
                public boolean apply(Attribute input) {
                    if (input.getDisplayName().equals(header)) {
                        return true;
                    }
                    return false;
                }

            });
            attrs.add(attr);
        }
        result.metadata.setAttributes(attrs);
        log.info("After sorting header list: " + result.metadata.getAttributes());
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
                        break;
                    }
                }
            }
            if (!foundMatchingAttribute) {
                attrIterator.remove();
            }
        }

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (!fieldMapping.isMappedToLatticeField()) {
                attributes.add(getAttributeFromFieldName(fieldMapping.getUserField(), fieldMapping.getFieldType()));
            }
        }

        if (fieldMappingDocument.getIgnoredFields() != null) {
            for (final String ignoredField : fieldMappingDocument.getIgnoredFields()) {
                if (ignoredField != null) {
                    Attribute attribute = Iterables.find(attributes, new Predicate<Attribute>() {
                        @Override
                        public boolean apply(@Nullable Attribute input) {
                            return ignoredField.equals(input.getDisplayName());
                        }
                    }, null);
                    if (attribute != null) {
                        attribute.setApprovedUsage(ApprovedUsage.NONE, ApprovedUsage.IGNORED);
                    }
                }
            }
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

            unknownColumn.setUserField(headerField);
            unknownColumn.setFieldType(getFieldTypeFromColumnContent(headerField));
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
        if (allowedDisplayNames != null) {
            for (int i = 0; i < attribute.getAllowedDisplayNames().size(); i++) {
                if (allowedDisplayNames.get(i).equalsIgnoreCase(header)) {
                    return true;
                }
            }
        }

        if (attribute.getDisplayName().equalsIgnoreCase(header)) {
            return true;
        }
        return false;
    }

    private Attribute getAttributeFromFieldName(String fieldName, UserDefinedType userDefinedType) {
        Attribute attribute = new Attribute();

        String fieldType;
        if (userDefinedType == null) {
            fieldType = getFieldTypeFromColumnContent(fieldName).getAvroType().toString().toLowerCase();
        } else {
            fieldType = userDefinedType.getAvroType().toString().toLowerCase();
        }

        attribute.setName(ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(fieldName));
        attribute.setPhysicalDataType(fieldType);
        attribute.setDisplayName(fieldName);
        attribute.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        attribute.setCategory(getCategoryBasedOnSchemaType(result.metadata.getInterpretation()));
        attribute.setFundamentalType(getFundamentalTypeFromFieldType(fieldType));
        attribute.setStatisticalType(getStatisticalTypeFromFieldType(fieldType));
        attribute.setNullable(true);
        attribute.setLogicalDataType(
                userDefinedType == UserDefinedType.DATE ? LogicalDataType.Date : attribute.getLogicalDataType());
        attribute.setTags(ModelingMetadata.INTERNAL_TAG);

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
        log.info(String.format("The statistical type is %s", statisticalType));
        return statisticalType;
    }

    private UserDefinedType getFieldTypeFromColumnContent(String columnHeaderName) {
        String mappedFieldName = ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(columnHeaderName);
        if (mappedFieldName.startsWith("Activity_Count_")) {
            return UserDefinedType.NUMBER;
        } else if (BOOLEAN_SET.contains(mappedFieldName)) {
            return UserDefinedType.BOOLEAN;
        } else if (STR_SET.contains(mappedFieldName)) {
            return UserDefinedType.TEXT;
        }

        UserDefinedType fundamentalType = null;

        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        List<String> columnFields = null;
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            InputStream is = fs.open(new Path(csvPath));
            columnFields = ValidateFileHeaderUtils.getCSVColumnValues(columnHeaderName, is, closeableResourcePool);

            log.info(String.format("column with header %s is: %s", columnHeaderName, columnFields.toString()));
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                throw new RuntimeException("Problem when closing the pool", e);
            }
        }
        if (columnFields.isEmpty()) {
            fundamentalType = UserDefinedType.TEXT;
        } else if (isBooleanTypeColumn(columnFields)) {
            fundamentalType = UserDefinedType.BOOLEAN;
        } else if (isDoubleTypeColumn(columnFields)) {
            fundamentalType = UserDefinedType.NUMBER;
        } else if (isDateTypeColumn(columnFields)) {
            fundamentalType = UserDefinedType.DATE;
        } else {
            fundamentalType = UserDefinedType.TEXT;
        }

        return fundamentalType;
    }

    private boolean isDateTypeColumn(List<String> columnFields) {
        String[] supportedDateFormat = { "YYYY-MM-DD", "YYYY-MM-DD'T'HH:mm:ss.sssZ" };
        for (String columnField : columnFields) {
            Date date = null;
            for (String format : supportedDateFormat) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat(format);
                    date = sdf.parse(columnField);
                    if (!columnField.equals(sdf.format(date))) {
                        date = null;
                    }
                } catch (ParseException ex) {
                }
                if (date != null) {
                    break;
                }
                date = null;
            }
            if (date == null) {
                return false;
            }
        }
        return true;
    }

    private boolean isBooleanTypeColumn(List<String> columnFields) {
        for (String columnField : columnFields) {
            if (columnField != null && !columnField.isEmpty()
                    && !ACCEPTED_BOOLEAN_VALUES.contains(columnField.toLowerCase())) {
                return false;
            }
        }
        return true;
    }

    private boolean isDoubleTypeColumn(List<String> columnFields) {
        for (String columnField : columnFields) {
            if (columnField != null && !columnField.isEmpty()) {
                try {
                    Double.parseDouble(columnField);
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
