package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InputValidatorWrapper;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SchemaInterpretationFunctionalInterface;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.RequiredType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.validation.ReservedField;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("modelingFileMetadataService")
public class ModelingFileMetadataServiceImpl implements ModelingFileMetadataService {
    private static final Logger log = LoggerFactory.getLogger(ModelingFileMetadataServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Override
    public FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName,
            SchemaInterpretation schemaInterpretation, ModelingParameters parameters) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }

        MetadataResolver resolver = getMetadataResolver(sourceFile, null, false);
        Table table = getTableFromParameters(sourceFile.getSchemaInterpretation());
        return resolver.getFieldMappingsDocumentBestEffort(table);
    }

    @Override
    public FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName, String entity, String source,
            String feedType) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        log.info(String.format("Customer Space: %s, entity: %s, source: %s, datafeed: %s", customerSpace.toString(),
                entity, source, feedType));
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entity);
        FieldMappingDocument fieldMappingFromSchemaRepo = getFieldMappingDocumentBestEffort(sourceFileName,
                schemaInterpretation, null);
        FieldMappingDocument resultDocument;
        if (dataFeedTask == null) {
            resultDocument = fieldMappingFromSchemaRepo;
        } else {
            Table templateTable = dataFeedTask.getImportTemplate();
            FieldMappingDocument fieldMappingFromTemplate = getFieldMappingBaseOnTable(sourceFile, templateTable);
            resultDocument = mergeFieldMappingBestEffort(fieldMappingFromTemplate, fieldMappingFromSchemaRepo);
        }
        return resultDocument;
    }

    private FieldMappingDocument mergeFieldMappingBestEffort(FieldMappingDocument templateMapping,
            FieldMappingDocument standardMapping) {
        Map<String, FieldMapping> standardMappingMap = new HashMap<>();
        for (FieldMapping fieldMapping : standardMapping.getFieldMappings()) {
            standardMappingMap.put(fieldMapping.getUserField(), fieldMapping);
        }
        for (FieldMapping fieldMapping : templateMapping.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                if (standardMappingMap.containsKey(fieldMapping.getUserField())
                        && standardMappingMap.get(fieldMapping.getUserField()).getMappedField() != null) {
                    fieldMapping.setMappedField(standardMappingMap.get(fieldMapping.getUserField()).getMappedField());
                    fieldMapping.setMappedToLatticeField(false);
                }
            }
        }
        return templateMapping;
    }

    private FieldMappingDocument getFieldMappingBaseOnTable(SourceFile sourceFile, Table table) {
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        return resolver.getFieldMappingsDocumentBestEffort(table);
    }

    private Table getTableFromParameters(SchemaInterpretation schemaInterpretation) {
        Table table = SchemaRepository.instance().getSchema(schemaInterpretation);
        SchemaInterpretationFunctionalInterface function = (interfaceName) -> {
            Attribute domainAttribute = table.getAttribute(interfaceName);
            if (domainAttribute != null) {
                domainAttribute.setNullable(Boolean.TRUE);
            }
        };
        schemaInterpretation.apply(function);
        return table;
    }

    @Override
    public void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument) {
        decodeFieldMapping(fieldMappingDocument);
        SourceFile sourceFile = getSourceFile(sourceFileName);
        Table table = getTableFromParameters(sourceFile.getSchemaInterpretation());
        resolveMetadata(sourceFile, fieldMappingDocument, table, false);
    }

    @Override
    public void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument, String entity,
            String source, String feedType) {
        decodeFieldMapping(fieldMappingDocument);
        regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity));
        if (BusinessEntity.getByName(entity).equals(BusinessEntity.Account)) {
            setCDLExternalSystems(fieldMappingDocument);
        }
        SourceFile sourceFile = getSourceFile(sourceFileName);
        Table table;
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        if (dataFeedTask == null) {
            table = getTableFromParameters(sourceFile.getSchemaInterpretation());
        } else {
            table = dataFeedTask.getImportTemplate();
        }
        resolveMetadata(sourceFile, fieldMappingDocument, table, true);
    }

    private void decodeFieldMapping(FieldMappingDocument fieldMappingDocument) {
        if (fieldMappingDocument == null || fieldMappingDocument.getFieldMappings() == null) {
            return;
        }
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            fieldMapping.setUserField(StringEscapeUtils.unescapeHtml4(fieldMapping.getUserField()));
        }
    }

    private void setCDLExternalSystems(FieldMappingDocument fieldMappingDocument) {
        if (fieldMappingDocument == null || fieldMappingDocument.getFieldMappings() == null
                || fieldMappingDocument.getFieldMappings().size() == 0) {
            return;
        }
        List<String> crmIds = new ArrayList<>();
        List<String> mapIds = new ArrayList<>();
        List<String> erpIds = new ArrayList<>();
        List<String> otherIds = new ArrayList<>();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getCdlExternalSystemType() != null) {
                fieldMapping.setMappedField(
                        ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(fieldMapping.getMappedField()));
                switch (fieldMapping.getCdlExternalSystemType()) {
                case CRM:
                    crmIds.add(fieldMapping.getMappedField());
                    break;
                case MAP:
                    mapIds.add(fieldMapping.getMappedField());
                    break;
                case ERP:
                    erpIds.add(fieldMapping.getMappedField());
                    break;
                case OTHER:
                    otherIds.add(fieldMapping.getMappedField());
                    break;
                }
            }
        }
        CDLExternalSystem originalSystem = cdlExternalSystemProxy.getCDLExternalSystem(
                MultiTenantContext.getCustomerSpace().toString());
        if (originalSystem == null) {
            CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
            cdlExternalSystem.setCRMIdList(crmIds);
            cdlExternalSystem.setMAPIdList(mapIds);
            cdlExternalSystem.setERPIdList(erpIds);
            cdlExternalSystem.setOtherIdList(otherIds);
            cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(),
                    cdlExternalSystem);
        } else {
            originalSystem.setCRMIdList(mergeList(originalSystem.getCRMIdList(), crmIds));
            originalSystem.setMAPIdList(mergeList(originalSystem.getMAPIdList(), mapIds));
            originalSystem.setERPIdList(mergeList(originalSystem.getERPIdList(), erpIds));
            originalSystem.setOtherIdList(mergeList(originalSystem.getOtherIdList(), otherIds));
            cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(),
                    originalSystem);
        }
    }

    private List<String> mergeList(List<String> list1, List<String> list2) {
        if (CollectionUtils.isEmpty(list1)) {
            return list2;
        } else if (CollectionUtils.isEmpty(list2)) {
            return list1;
        }
        List<String> merged = new ArrayList<>();
        merged.addAll(list1);
        Set<String> list1Set = new HashSet<>(list1);
        list2.forEach(item -> {
            if (!list1Set.contains(item)) {
                list1Set.add(item);
                merged.add(item);
            }
        });
        return merged;
    }

    private void regulateFieldMapping(FieldMappingDocument fieldMappingDocument, BusinessEntity entity) {
        if (fieldMappingDocument == null || fieldMappingDocument.getFieldMappings() == null
                || fieldMappingDocument.getFieldMappings().size() == 0) {
            return;
        }
        // 1.set external system column name
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getCdlExternalSystemType() != null) {
                if (!fieldMapping.getMappedField().toUpperCase().endsWith("ID")) {
                    fieldMapping.setMappedField(fieldMapping.getMappedField() + "ID");
                }
            }
        }
        Table standardTable = SchemaRepository.instance().getSchema(entity);
        Set<String> reservedName = standardTable.getAttributes().stream()
                .map(Attribute::getName)
                .collect(Collectors.toSet());
        Set<String> mappedFieldName = new HashSet<>();
        Set<String> pendingUserFieldName = new HashSet<>();
        Set<String> mappedUserFieldName = new HashSet<>();
        // 2.check if there's multiple mapping to standard field
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                pendingUserFieldName.add(fieldMapping.getUserField());
            } else {
                mappedUserFieldName.add(fieldMapping.getUserField());
                if (reservedName.contains(fieldMapping.getMappedField())) {
                    if (mappedFieldName.contains(fieldMapping.getMappedField())) {
                        throw new RuntimeException(String.format("Cannot map two column to %s, please re-map column",
                                fieldMapping.getMappedField()));
                    } else {
                        fieldMapping.setMappedToLatticeField(true);
                        mappedFieldName.add(fieldMapping.getMappedField());
                    }
                }
            }
        }
        // 3.remove extra unmapped field
        Iterator<FieldMapping> fmIterator = fieldMappingDocument.getFieldMappings().iterator();
        while (fmIterator.hasNext()) {
            FieldMapping fieldMapping = fmIterator.next();
            if (pendingUserFieldName.contains(fieldMapping.getUserField())) {
                if (mappedUserFieldName.contains(fieldMapping.getUserField())) {
                    if (fieldMapping.getMappedField() == null) {
                        fmIterator.remove();
                    }
                } else {
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);
                }
            }

        }
    }

    private void resolveMetadata(SourceFile sourceFile, FieldMappingDocument fieldMappingDocument, Table table,
            boolean cdlResolve) {
        MetadataResolver resolver = getMetadataResolver(sourceFile, fieldMappingDocument, cdlResolve);

        log.info(String.format("the ignored fields are: %s", fieldMappingDocument.getIgnoredFields()));
        if (!resolver.isFieldMappingDocumentFullyDefined()) {
            throw new RuntimeException(
                    String.format("Metadata is not fully defined for file %s", sourceFile.getName()));
        }
        resolver.calculateBasedOnFieldMappingDocument(table);

        String customerSpace = MultiTenantContext.getCustomerSpace().toString();

        if (sourceFile.getTableName() != null) {
            metadataProxy.deleteTable(customerSpace, sourceFile.getTableName());
        }

        Table newTable = resolver.getMetadata();
        newTable.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
        metadataProxy.createTable(customerSpace, newTable.getName(), newTable);
        sourceFile.setTableName(newTable.getName());
        sourceFileService.update(sourceFile);
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, CloseableResourcePool closeableResourcePool,
            String fileDisplayName, boolean checkHeaderFormat) {

        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }

        stream.mark(1024 * 500);

        Set<String> headerFields = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        try {
            stream.reset();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        if (checkHeaderFormat) {
            ValidateFileHeaderUtils.checkForHeaderFormat(headerFields);
        }
        ValidateFileHeaderUtils.checkForEmptyHeaders(fileDisplayName, headerFields);
        ValidateFileHeaderUtils.checkForLongHeaders(headerFields);
        Collection<String> reservedWords = Arrays.asList(ReservedField.Rating.displayName, ReservedField.Percentile.displayName);
        Collection<String> reservedBeginings = Arrays.asList(DataCloudConstants.CEAttr, DataCloudConstants.EAttr);
        ValidateFileHeaderUtils.checkForReservedHeaders(fileDisplayName, headerFields, reservedWords,
                reservedBeginings);
        return stream;
    }

    @Override
    public Map<SchemaInterpretation, List<LatticeSchemaField>> getSchemaToLatticeSchemaFields(
            boolean excludeLatticeDataAttributes) {
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

    @Override
    public List<LatticeSchemaField> getSchemaToLatticeSchemaFields(SchemaInterpretation schemaInterpretation) {
        List<LatticeSchemaField> latticeSchemaFields = new ArrayList<>();
        List<Attribute> attributes = SchemaRepository.instance().getSchema(schemaInterpretation).getAttributes();
        for (Attribute accountAttribute : attributes) {
            latticeSchemaFields.add(getLatticeFieldFromTableAttribute(accountAttribute));
        }
        return latticeSchemaFields;
    }

    @Override
    public List<LatticeSchemaField> getSchemaToLatticeSchemaFields(String entity, String source, String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        if (dataFeedTask == null) {
            SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entity);
            return getSchemaToLatticeSchemaFields(schemaInterpretation);
        } else {
            List<LatticeSchemaField> templateSchemaFields = new ArrayList<>();
            List<Attribute> attributes = dataFeedTask.getImportTemplate().getAttributes();
            for (Attribute accountAttribute : attributes) {
                templateSchemaFields.add(getLatticeFieldFromTableAttribute(accountAttribute));
            }
            return templateSchemaFields;
        }
    }

    private LatticeSchemaField getLatticeFieldFromTableAttribute(Attribute attribute) {
        LatticeSchemaField latticeSchemaField = new LatticeSchemaField();

        latticeSchemaField.setName(attribute.getName());
        // latticeSchemaField.setFieldType(attribute.getPhysicalDataType());
        latticeSchemaField.setFieldType(MetadataResolver.getFieldTypeFromPhysicalType(attribute.getPhysicalDataType()));
        if (attribute.getRequired()) {
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

    private MetadataResolver getMetadataResolver(SourceFile sourceFile, FieldMappingDocument fieldMappingDocument,
            boolean cdlResolve) {
        return new MetadataResolver(sourceFile.getPath(), yarnConfiguration, fieldMappingDocument, cdlResolve);
    }
}
