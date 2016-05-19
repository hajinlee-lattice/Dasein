package com.latticeengines.pls.metadata.resolution;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;

public class NewMetadataResolver {
    private String csvPath;
    private SchemaInterpretation schema;
    private FieldMappingDocument fieldMappingDocument;
    private Configuration yarnConfiguration;

    private static class Result {
        public List<FieldMapping> fieldMappings;
        public Table metadata;
    }

    private Result result;

    public NewMetadataResolver(String csvPath, Configuration yarnConfiguration, FieldMappingDocument fieldMappingDocument) {
        this.csvPath = csvPath;
        this.yarnConfiguration = yarnConfiguration;
        this.fieldMappingDocument = fieldMappingDocument;
        result = new Result();
    }

    public void calculateBasedOnExistingMetadata(Table metadataTable) {
        result.metadata = metadataTable;
        result.metadata.getExtracts().clear();
        result.fieldMappings = new ArrayList<>();
        calculateHelper();
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

    public FieldMappingDocument getFieldMappingsDocumentBestEffort() {
        FieldMappingDocument fieldMappingsDocument = new FieldMappingDocument();

        schema = schemaInterpretation();
        fieldMappingsDocument.setSchemaInterpretation(schema);
        calculate();
        fieldMappingsDocument.setFieldMappings(result.fieldMappings);

        return fieldMappingsDocument;
    }

    public void resolveBasedOnFieldMappingDocument() {
        schema = this.fieldMappingDocument.getSchemaInterpretation();
        calculateBasedOnFieldMappingDocument();
    }

    public void setSchema(SchemaInterpretation schema) {
        this.schema = schema;
    }

    private void calculateBasedOnFieldMappingDocument() {
        SchemaRepository repository = SchemaRepository.instance();
        result.metadata = repository.getSchema(schema);
        result.fieldMappings = new ArrayList<>();

        List<Attribute> attributes = result.metadata.getAttributes();

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.isMappedToLatticeField()) {
                for (Attribute attribute : attributes) {
                    if (attribute.getDisplayName().equals(fieldMapping.getMappedField())) {
                        attribute.setDisplayName(fieldMapping.getUserField());
                    }
                }
            } else {
                attributes.add(generateAttributeFromFieldName(fieldMapping.getUserField()));
            }
        }

        for (String ignoredField : fieldMappingDocument.getIgnoredFields()) {
            Attribute attribute = generateAttributeFromFieldName(ignoredField);
            attribute.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            attributes.add(attribute);
        }
    }

    public void calculate() {
        SchemaRepository repository = SchemaRepository.instance();
        result.metadata = repository.getSchema(schema);
        result.fieldMappings = new ArrayList<>();
        calculateHelper();
    }

    public SchemaInterpretation schemaInterpretation() {
        if (schema == null) {
            Set<String> headerFields = getHeaderFields();
            if (headerFields.contains("Website")) {
                schema = SchemaInterpretation.SalesforceAccount;
            } else if (headerFields.contains("Email")) {
                schema = SchemaInterpretation.SalesforceLead;
            }
        }

        if (schema == null) {
            return SchemaInterpretation.SalesforceLead;
        }
        return schema;
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
            String header = new String();
            FieldMapping knownColumn = new FieldMapping();
            while (headerIterator.hasNext()) {
                header = headerIterator.next();
                if (attribute.getAllowedDisplayNames() != null && attribute.getAllowedDisplayNames().contains(header)) {
                    headerIterator.remove();
                    attribute.setDisplayName(header);
                    foundMatchingAttribute = true;
                } else if (attribute.getDisplayName().equals(header)) {
                    headerIterator.remove();
                    foundMatchingAttribute = true;
                }
            }
            if (!foundMatchingAttribute) {
                if (!attribute.isNullable()) {
                    missingRequiredFields.add(attribute.getName());
                }
                attrIterator.remove();
            } else {
                knownColumn.setUserField(header);
                knownColumn.setMappedField(attribute.getDisplayName());
                knownColumn.setFieldType(attribute.getDataType());
                knownColumn.setMappedToLatticeField(true);

                result.fieldMappings.add(knownColumn);
            }
        }

        // Add columns that are not in metadata to unknown columns
        for (final String headerField : headerFields) {
            FieldMapping unknownColumn = new FieldMapping();

            unknownColumn.setUserField(headerField);
            unknownColumn.setFieldType(Schema.Type.STRING.toString());
            unknownColumn.setMappedToLatticeField(false);

            result.fieldMappings.add(unknownColumn);
        }

        Attribute lastModified = result.metadata.getAttribute(InterfaceName.LastModifiedDate);
        if (lastModified == null) {
            result.metadata.setLastModifiedKey(null);
        }
    }

    Attribute generateAttributeFromFieldName(String fieldName) {
        Attribute attribute = new Attribute();

        String dataType = generateFundamentalTypeFromColumnContent(fieldName);
        attribute.setName(fieldName.replaceAll("[^A-Za-z0-9_]", "_"));
        attribute.setPhysicalDataType(dataType);
        attribute.setDisplayName(dataType);
        attribute.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        attribute.setCategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        attribute.setFundamentalType(dataType);
        attribute.setStatisticalType(dataType);
        attribute.setNullable(true);
        attribute.setTags(ModelingMetadata.INTERNAL_TAG);

        return attribute;
    }

    private String generateFundamentalTypeFromColumnContent(String columnHeaderName) {
        String fundamentalType = null;

        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        List<String> columnFields = new ArrayList<>();
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            InputStream is = fs.open(new Path(csvPath));
            columnFields = ValidateFileHeaderUtils.getCSVColumnFields(columnHeaderName, is, closeableResourcePool);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                throw new RuntimeException("Problem when closing the pool", e);
            }
        }
        if (isBooleanTypeColumn(columnFields)) {
            fundamentalType = ModelingMetadata.FT_BOOLEAN;
        } else if (isDoubleTypeColumn(columnFields)) {
            fundamentalType = ModelingMetadata.FT_NUMERIC;
        } else {
            fundamentalType = ModelingMetadata.FT_ALPHA;
        }

        return fundamentalType;
    }

    private static List<String> ACCEPTED_BOOLEAN_VALUES = Arrays.asList("true", "false");
    private boolean isBooleanTypeColumn(List<String> columnFields) {
        for (String columnField : columnFields) {
            if (! ACCEPTED_BOOLEAN_VALUES.contains(columnField.toLowerCase())) {
                return false;
            }
        }
        return true;
    }

    private boolean isDoubleTypeColumn(List<String> columnFields) {
        for (String columnField : columnFields) {
            try {
                Double.parseDouble(columnField);
            } catch (NumberFormatException e) {
                return false;
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
}
