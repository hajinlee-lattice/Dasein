package com.latticeengines.pls.metadata.resolution;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;

public class MetadataResolver {
    private static Logger log = Logger.getLogger(MetadataResolver.class);
    private static List<String> ACCEPTED_BOOLEAN_VALUES = Arrays.asList("true", "false", "1", "0");

    private static final Set<String> BOOLEAN_SET = Sets.newHashSet(new String[] { "Interest_esb__c",
            "Interest_tcat__c", "kickboxAcceptAll", "Free_Email_Address__c", "kickboxFree", "Unsubscribed",
            "kickboxDisposable", "HasAnypointLogin", "HasCEDownload", "HasEEDownload" });
    private static final Set<String> STR_SET = Sets.newHashSet(new String[] { "Lead_Source_Asset__c", "kickboxStatus",
            "SICCode", "Source_Detail__c", "Cloud_Plan__c" });

    private String csvPath;
    private SchemaInterpretation schema;
    private FieldMappingDocument fieldMappingDocument;
    private Configuration yarnConfiguration;
    private Table metadata;

    private static class Result {
        public List<FieldMapping> fieldMappings;
        public Table metadata;
    }

    private Result result;

    public MetadataResolver(String csvPath, SchemaInterpretation schemaInterpretation, Configuration yarnConfiguration,
            FieldMappingDocument fieldMappingDocument) {
        this.csvPath = csvPath;
        this.schema = schemaInterpretation;
        this.yarnConfiguration = yarnConfiguration;
        this.fieldMappingDocument = fieldMappingDocument;
        result = new Result();
    }

    public MetadataResolver(String csvPath, Table metadata, Configuration yarnConfiguration,
            FieldMappingDocument fieldMappingDocument) {
        this.metadata = metadata;
        this.fieldMappingDocument = fieldMappingDocument;
        result = new Result();
    }

    public FieldMappingDocument getFieldMappingsDocumentBestEffort() {
        FieldMappingDocument fieldMappingsDocument = new FieldMappingDocument();

        calculate();
        fieldMappingsDocument.setFieldMappings(result.fieldMappings);

        return fieldMappingsDocument;
    }

    public void calculateBasedOnFieldMappingDocumentAndTable() {
        result.metadata = metadata;
        calculateBasedOnMetadta();
    }

    public void calculateBasedOnFieldMappingDocument() {
        SchemaRepository repository = SchemaRepository.instance();
        result.metadata = repository.getSchema(schema);
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
                            return ignoredField.equals(input.getName());
                        }
                    }, null);
                    if (attribute != null) {
                        attribute.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
                    }
                }
            }
        }
        Attribute lastModified = result.metadata.getAttribute(InterfaceName.LastModifiedDate);
        if (lastModified == null) {
            result.metadata.setLastModifiedKey(null);
        }
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

    private void calculate() {
        SchemaRepository repository = SchemaRepository.instance();
        result.metadata = repository.getSchema(schema);
        result.fieldMappings = new ArrayList<>();
        calculateHelper();
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

    private UserDefinedType getFieldTypeFromPhysicalType(String attributeType) {
        UserDefinedType fieldType;
        switch (attributeType.toUpperCase()) {
        case "BOOLEAN":
            fieldType = UserDefinedType.BOOLEAN;
            break;
        case "LONG":
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

        log.info(String.format("The fieldType is: %s", fieldType));
        attribute.setName(fieldName.replaceAll("[^A-Za-z0-9_]", "_"));
        attribute.setPhysicalDataType(fieldType);
        attribute.setDisplayName(fieldName);
        attribute.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        attribute.setCategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        attribute.setFundamentalType(getFundamentalTypeFromFieldType(fieldType));
        attribute.setStatisticalType(getStatisticalTypeFromFieldType(fieldType));
        attribute.setNullable(true);
        attribute.setTags(ModelingMetadata.INTERNAL_TAG);

        return attribute;
    }

    private String getFundamentalTypeFromFieldType(String fieldType) {
        String fundamentalType = null;
        switch (fieldType.toUpperCase()) {
        case "BOOLEAN":
            fundamentalType = ModelingMetadata.FT_BOOLEAN;
            break;
        case "NUMBER":
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
        case "NUMBER":
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
        String mappedFieldName = columnHeaderName.replaceAll("[^A-Za-z0-9_]", "_");
        if (mappedFieldName.startsWith("Activity_Count_")) {
            return UserDefinedType.NUMBER;
        } else if (BOOLEAN_SET.contains(mappedFieldName)) {
            return UserDefinedType.BOOLEAN;
        } else if (STR_SET.contains(mappedFieldName)) {
            return UserDefinedType.TEXT;
        }

        UserDefinedType fundamentalType = null;

        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        List<String> columnFields = new ArrayList<>();
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
        } else {
            fundamentalType = UserDefinedType.TEXT;
        }

        return fundamentalType;
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
