package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InputValidatorWrapper;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.RequiredType;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("modelingFileMetadataService")
public class ModelingFileMetadataServiceImpl implements ModelingFileMetadataService {
    private static final Log log = LogFactory.getLog(ModelingFileMetadataServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName,
            SchemaInterpretation schemaInterpretation) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }
        MetadataResolver resolver = getMetadataResolver(sourceFile, schemaInterpretation, null);
        return resolver.getFieldMappingsDocumentBestEffort();
    }

    @Override
    public void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        MetadataResolver resolver = getMetadataResolver(sourceFile, sourceFile.getSchemaInterpretation(),
                fieldMappingDocument);

        log.info(String.format("the ignored fields are: %s", fieldMappingDocument.getIgnoredFields()));
        if (!resolver.isFieldMappingDocumentFullyDefined()) {
            throw new RuntimeException(String.format("Metadata is not fully defined for file %s", sourceFileName));
        }
        resolver.calculateBasedOnFieldMappingDocument();

        String customerSpace = MultiTenantContext.getTenant().getId().toString();

        if (sourceFile.getTableName() != null) {
            metadataProxy.deleteTable(customerSpace, sourceFile.getTableName());
        }

        Table table = resolver.getMetadata();
        table.setName("SourceFile_" + sourceFileName.replace(".", "_"));
        metadataProxy.createTable(customerSpace, table.getName(), table);
        sourceFile.setTableName(table.getName());
        sourceFileService.update(sourceFile);
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, SchemaInterpretation schema,
            CloseableResourcePool closeableResourcePool, String fileDisplayName) {

        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }

        stream.mark(1024 * 500);

        Set<String> headerFields = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        try {
            stream.reset();
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        SchemaRepository repository = SchemaRepository.instance();
        Table metadata = repository.getSchema(schema);
        List<Attribute> attributes = metadata.getAttributes();

        ValidateFileHeaderUtils.checkForMissingRequiredFields(attributes, fileDisplayName, headerFields, true);
        ValidateFileHeaderUtils.checkForDuplicateHeaders(attributes, fileDisplayName, headerFields);
        return stream;
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, CloseableResourcePool closeableResourcePool,
            String fileDisplayName) {

        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }

        stream.mark(1024 * 500);

        Set<String> headerFields = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        try {
            stream.reset();
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }

        ValidateFileHeaderUtils.checkForEmptyHeaders(fileDisplayName, headerFields);
        return stream;
    }

    @Override
    public Map<SchemaInterpretation, List<LatticeSchemaField>> getSchemaToLatticeSchemaFields() {
        Map<SchemaInterpretation, List<LatticeSchemaField>> schemaToLatticeSchemaFields = new HashMap<>();

        List<Attribute> accountAttributes = SchemaRepository.instance()
                .getSchema(SchemaInterpretation.SalesforceAccount).getAttributes();
        List<LatticeSchemaField> latticeAccountSchemaFields = new ArrayList<>();
        for (Attribute accountAttribute : accountAttributes) {
            latticeAccountSchemaFields.add(getLatticeFieldFromTableAttribute(accountAttribute));
        }
        schemaToLatticeSchemaFields.put(SchemaInterpretation.SalesforceAccount, latticeAccountSchemaFields);

        List<Attribute> leadAttributes = SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceLead)
                .getAttributes();
        List<LatticeSchemaField> latticeLeadSchemaFields = new ArrayList<>();
        for (Attribute leadAttribute : leadAttributes) {
            latticeLeadSchemaFields.add(getLatticeFieldFromTableAttribute(leadAttribute));
        }
        schemaToLatticeSchemaFields.put(SchemaInterpretation.SalesforceLead, latticeLeadSchemaFields);

        return schemaToLatticeSchemaFields;
    }

    private LatticeSchemaField getLatticeFieldFromTableAttribute(Attribute attribute) {
        LatticeSchemaField latticeSchemaField = new LatticeSchemaField();

        latticeSchemaField.setName(attribute.getName());
        // latticeSchemaField.setFieldType(attribute.getPhysicalDataType());
        latticeSchemaField.setFieldType(getFieldTypeFromPhysicalType(attribute.getPhysicalDataType()));
        if (!attribute.getNullable()) {
            latticeSchemaField.setRequiredType(RequiredType.Required);
        } else if (!attribute.getValidatorWrappers().isEmpty()) {
            String requiredIfNoField = getRequiredIfNoField(attribute.getValidatorWrappers());
            if (requiredIfNoField != null) {
                latticeSchemaField.setRequiredType(RequiredType.RequiredIfOtherFieldIsEmpty);
                latticeSchemaField.setRequiredIfNoField(requiredIfNoField);
            } else {
                latticeSchemaField.setRequiredType(RequiredType.NotRequired);
            }
        } else {
            latticeSchemaField.setRequiredType(RequiredType.NotRequired);
        }

        return latticeSchemaField;
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

    private String getRequiredIfNoField(List<InputValidatorWrapper> inputValidatorWrappers) {
        for (InputValidatorWrapper inputValidatorWrapper : inputValidatorWrappers) {
            InputValidator inputValidator = inputValidatorWrapper.getValidator();

            if (inputValidator.getClass() == RequiredIfOtherFieldIsEmpty.class) {
                return ((RequiredIfOtherFieldIsEmpty) inputValidator).otherField;
            }
        }

        return null;
    }

    private SourceFile getSourceFile(String sourceFileName) {
        SourceFile sourceFile = sourceFileService.findByName(sourceFileName);
        if (sourceFile == null) {
            throw new RuntimeException(String.format("Could not locate source file with name %s", sourceFileName));
        }
        return sourceFile;
    }

    private MetadataResolver getMetadataResolver(SourceFile sourceFile, SchemaInterpretation schemaInterpretation,
            FieldMappingDocument fieldMappingDocument) {
        return new MetadataResolver(sourceFile.getPath(), schemaInterpretation, yarnConfiguration, fieldMappingDocument);
    }
}
