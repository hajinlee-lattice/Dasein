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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InputValidatorWrapper;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SchemaInterpretationFunctionalInterface;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.ExtraFieldMappingInfo;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.RequiredType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.validation.ReservedField;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.CDLService;
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
    private BatonService batonService;

    @Autowired
    private CDLService cdlService;

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Override
    public FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName,
            SchemaInterpretation schemaInterpretation, ModelingParameters parameters, boolean withoutId,
            boolean enableEntityMatch) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }

        MetadataResolver resolver = getMetadataResolver(sourceFile, null, false);
        Table table = getTableFromParameters(sourceFile.getSchemaInterpretation(), withoutId, enableEntityMatch);
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
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        Table table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId, enableEntityMatch);
        FieldMappingDocument fieldMappingFromSchemaRepo = resolver.getFieldMappingsDocumentBestEffort(table);
        generateExtraFieldMappingInfo(fieldMappingFromSchemaRepo, true);
        FieldMappingDocument resultDocument;
        if (dataFeedTask == null) {
            resultDocument = fieldMappingFromSchemaRepo;
        } else {
            Table templateTable = dataFeedTask.getImportTemplate();
            FieldMappingDocument fieldMappingFromTemplate = getFieldMappingBaseOnTable(sourceFile, templateTable);
            resultDocument = mergeFieldMappingBestEffort(fieldMappingFromTemplate, fieldMappingFromSchemaRepo,
                    templateTable, SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true,
                            withoutId, enableEntityMatch));
        }
        return resultDocument;
    }

    private void generateExtraFieldMappingInfo(FieldMappingDocument fieldMappingDocument, boolean standard) {
        if (fieldMappingDocument == null || CollectionUtils.isEmpty(fieldMappingDocument.getFieldMappings())) {
            return;
        }
        ExtraFieldMappingInfo extraFieldMappingInfo = new ExtraFieldMappingInfo();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (!standard && fieldMapping.isMappedToLatticeField()) {
                extraFieldMappingInfo.addExistingMapping(fieldMapping);
            } else {
                if (StringUtils.isEmpty(fieldMapping.getMappedField())) {
                    extraFieldMappingInfo.addNewMappings(fieldMapping);
                }
            }
        }
        fieldMappingDocument.setExtraFieldMappingInfo(extraFieldMappingInfo);
    }

    private FieldMappingDocument mergeFieldMappingBestEffort(FieldMappingDocument templateMapping,
            FieldMappingDocument standardMapping, Table templateTable, Table standardTable) {
        Map<String, FieldMapping> standardMappingMap = new HashMap<>();
        for (FieldMapping fieldMapping : standardMapping.getFieldMappings()) {
            standardMappingMap.put(fieldMapping.getUserField(), fieldMapping);
        }
        Set<String> alreadyMappedField = templateMapping.getFieldMappings().stream()
                                                        .map(FieldMapping::getMappedField)
                                                        .filter(Objects::nonNull).collect(Collectors.toSet());
        for (FieldMapping fieldMapping : templateMapping.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                if (standardMappingMap.containsKey(fieldMapping.getUserField())
                        && standardMappingMap.get(fieldMapping.getUserField()).getMappedField() != null
                        && !alreadyMappedField.contains(standardMappingMap.get(fieldMapping.getUserField()).getMappedField())) {
                    fieldMapping.setMappedField(standardMappingMap.get(fieldMapping.getUserField()).getMappedField());
                    fieldMapping.setFieldType(standardMappingMap.get(fieldMapping.getUserField()).getFieldType());
                    fieldMapping.setMappedToLatticeField(false);
                }
            }
        }
        // newMappings will be determined
        generateExtraFieldMappingInfo(templateMapping, false);
        // filter existingMappings
        Map<String, Attribute> standardAttrs =
                standardTable.getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));
        templateMapping.getExtraFieldMappingInfo().setExistingMappings(
                templateMapping.getExtraFieldMappingInfo().getExistingMappings().stream()
                        .filter(fieldMapping -> !standardAttrs.containsKey(fieldMapping.getMappedField()))
                        .collect(Collectors.toList()));
        // add missedMappings
        Map<String, Attribute> templateAttrs =
                templateTable.getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));
        Set<String> mappedAttr =
                templateMapping.getFieldMappings().stream()
                        .filter(fieldMapping -> StringUtils.isNotEmpty(fieldMapping.getMappedField()))
                        .map(FieldMapping::getMappedField)
                        .collect(Collectors.toSet());
        Map<String, Attribute> missedAttrs = new HashMap<>();
        templateAttrs.forEach((key, value) -> {
            if (!mappedAttr.contains(key) && !standardAttrs.containsKey(key)) {
                missedAttrs.put(key, value);
            }
        });
        missedAttrs.forEach((key, value) -> {
            FieldMapping fieldMapping = new FieldMapping();
            fieldMapping.setUserField(value.getDisplayName());
            fieldMapping.setMappedField(value.getName());
            fieldMapping.setFieldType(MetadataResolver.getFieldTypeFromPhysicalType(value.getPhysicalDataType()));
            templateMapping.getExtraFieldMappingInfo().addMissedMapping(fieldMapping);
        });
        return templateMapping;
    }

    private FieldMappingDocument getFieldMappingBaseOnTable(SourceFile sourceFile, Table table) {
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        return resolver.getFieldMappingsDocumentBestEffort(table);
    }

    private Table getTableFromParameters(SchemaInterpretation schemaInterpretation, boolean withoutId,
                                         boolean enableEntityMatch) {
        Table table = SchemaRepository.instance().getSchema(schemaInterpretation, withoutId, enableEntityMatch);
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
        Table table = getTableFromParameters(sourceFile.getSchemaInterpretation(), false, false);
        resolveMetadata(sourceFile, fieldMappingDocument, table, false, null, null);
    }

    @Override
    public void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument, String entity,
            String source, String feedType) {
        decodeFieldMapping(fieldMappingDocument);
        SourceFile sourceFile = getSourceFile(sourceFileName);
        Table table, schemaTable;
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        if (dataFeedTask == null) {
            table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId, enableEntityMatch);
            regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity), feedType, null);
        } else {
            table = dataFeedTask.getImportTemplate();
            regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity), feedType, table);
        }
        schemaTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId, enableEntityMatch);
        resolveMetadata(sourceFile, fieldMappingDocument, table, true, schemaTable, BusinessEntity.getByName(entity));
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, CloseableResourcePool leCsvParser, String fileName,
            boolean checkHeaderFormat) {
        return validateHeaderFields(stream, leCsvParser, fileName, checkHeaderFormat, false);
    }

    private void decodeFieldMapping(FieldMappingDocument fieldMappingDocument) {
        if (fieldMappingDocument == null || fieldMappingDocument.getFieldMappings() == null) {
            return;
        }
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            fieldMapping.setUserField(StringEscapeUtils.unescapeHtml4(fieldMapping.getUserField()));
        }
    }

    private List<String> mergeList(List<String> list1, List<String> list2) {
        if (CollectionUtils.isEmpty(list1)) {
            return list2;
        } else if (CollectionUtils.isEmpty(list2)) {
            return list1;
        }
        List<String> merged = new ArrayList<>(list1);
        Set<String> list1Set = new HashSet<>(list1);
        list2.forEach(item -> {
            if (!list1Set.contains(item)) {
                list1Set.add(item);
                merged.add(item);
            }
        });
        return merged;
    }

    private void regulateFieldMapping(FieldMappingDocument fieldMappingDocument, BusinessEntity entity, String feedType,
            Table templateTable) {
        if (fieldMappingDocument == null || fieldMappingDocument.getFieldMappings() == null
                || fieldMappingDocument.getFieldMappings().size() == 0) {
            return;
        }
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        // 1. set system related mapping
        FieldMapping customerLatticeId = null;
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getIdType() != null) {
                String systemName = cdlService.getSystemNameFromFeedType(feedType);
                if (StringUtils.isNotEmpty(systemName)) {
                    if (StringUtils.isEmpty(fieldMapping.getSystemName()) || systemName.equals(fieldMapping.getSystemName())) {
                        fieldMapping.setFieldType(UserDefinedType.TEXT);
                        S3ImportSystem importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                        switch (fieldMapping.getIdType()) {
                            case Account:
                                String accountSystemId = importSystem.getAccountSystemId();
                                if (fieldMapping.isMapToLatticeId()) {
                                    importSystem.setMapToLatticeAccount(true);
                                    cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                                    importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                                }
                                if (StringUtils.isEmpty(accountSystemId)) {
                                    accountSystemId = importSystem.generateAccountSystemId();
                                    importSystem.setAccountSystemId(accountSystemId);
                                    importSystem.setMapToLatticeAccount(fieldMapping.isMapToLatticeId());
                                    cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                                    fieldMapping.setMappedToLatticeField(false);
                                }
                                fieldMapping.setMappedField(accountSystemId);
                                if (fieldMapping.isMapToLatticeId()) {
                                    customerLatticeId = new FieldMapping();
                                    customerLatticeId.setUserField(fieldMapping.getUserField());
                                    customerLatticeId.setMappedField(InterfaceName.CustomerAccountId.name());
                                    customerLatticeId.setFieldType(fieldMapping.getFieldType());
                                }
                                break;
                            case Contact:
                                String contactSystemId = importSystem.getContactSystemId();
                                if (fieldMapping.isMapToLatticeId()) {
                                    importSystem.setMapToLatticeContact(true);
                                    cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                                    importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                                }
                                if (StringUtils.isEmpty(contactSystemId)) {
                                    contactSystemId = importSystem.generateContactSystemId();
                                    importSystem.setContactSystemId(contactSystemId);
                                    importSystem.setMapToLatticeContact(fieldMapping.isMapToLatticeId());
                                    cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                                }
                                fieldMapping.setMappedField(contactSystemId);
                                if (fieldMapping.isMapToLatticeId()) {
                                    customerLatticeId = new FieldMapping();
                                    customerLatticeId.setUserField(fieldMapping.getUserField());
                                    customerLatticeId.setMappedField(InterfaceName.CustomerContactId.name());
                                    customerLatticeId.setFieldType(fieldMapping.getFieldType());
                                }
                                break;
                            default:
                                throw new IllegalArgumentException("Unrecognized idType: " + fieldMapping.getIdType());
                        }
                    } else {
                        S3ImportSystem importSystem = cdlService.getS3ImportSystem(customerSpace.toString(),
                                fieldMapping.getSystemName());
                        if (importSystem == null) {
                            throw new IllegalArgumentException("Cannot find Import System: " + fieldMapping.getSystemName());
                        }
                        switch (fieldMapping.getIdType()) {
                            case Account:
                                if (StringUtils.isEmpty(importSystem.getAccountSystemId())) {
                                    throw new IllegalArgumentException(String.format("System %s does not have system " +
                                            "account id!", importSystem.getDisplayName()));
                                }
                                fieldMapping.setFieldType(UserDefinedType.TEXT);
                                fieldMapping.setMappedField(importSystem.getAccountSystemId());
                                fieldMapping.setMappedToLatticeField(false);
                                if (importSystem.isMapToLatticeAccount()) {
                                    customerLatticeId = new FieldMapping();
                                    customerLatticeId.setUserField(fieldMapping.getUserField());
                                    customerLatticeId.setMappedField(InterfaceName.CustomerAccountId.name());
                                    customerLatticeId.setFieldType(fieldMapping.getFieldType());
                                }
                                break;
                            case Contact:
                                if (StringUtils.isEmpty(importSystem.getContactSystemId())) {
                                    throw new IllegalArgumentException(String.format("System %s does not have system " +
                                            "contact id!", importSystem.getDisplayName()));
                                }
                                fieldMapping.setFieldType(UserDefinedType.TEXT);
                                fieldMapping.setMappedField(importSystem.getContactSystemId());
                                fieldMapping.setMappedToLatticeField(false);
                                if (importSystem.isMapToLatticeContact()) {
                                    customerLatticeId = new FieldMapping();
                                    customerLatticeId.setUserField(fieldMapping.getUserField());
                                    customerLatticeId.setMappedField(InterfaceName.CustomerContactId.name());
                                    customerLatticeId.setFieldType(fieldMapping.getFieldType());
                                }
                                break;
                            default:
                                throw new IllegalArgumentException("Unrecognized idType: " + fieldMapping.getIdType());
                        }
                    }
                }
            }
        }
        // map customer lattice id
        if (customerLatticeId != null) {
            boolean existFromTemplate = false;
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (customerLatticeId.getMappedField().equals(fieldMapping.getMappedField())) {
                    fieldMapping.setUserField(customerLatticeId.getUserField());
                    fieldMapping.setFieldType(customerLatticeId.getFieldType());
                    existFromTemplate = true;
                }
            }
            if (!existFromTemplate) {
                customerLatticeId.setMappedToLatticeField(false);
                fieldMappingDocument.getFieldMappings().add(customerLatticeId);
            }
        } else {
            Iterator<FieldMapping> fmIterator = fieldMappingDocument.getFieldMappings().iterator();
            while (fmIterator.hasNext()) {
                FieldMapping fieldMapping = fmIterator.next();
                if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())
                        || InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField())) {
                    fmIterator.remove();
                    break;
                }
            }
        }
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        Table standardTable = templateTable == null ? SchemaRepository.instance().getSchema(entity, true, withoutId, enableEntityMatch)
                : templateTable;
        Set<String> reservedName = standardTable.getAttributes().stream().map(Attribute::getName)
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
                        throw new LedpException(LedpCode.LEDP_18196, new String[] { fieldMapping.getMappedField() });
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
        // 4. Set External Id field mapToLattice flag
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getCdlExternalSystemType() != null) {
                fieldMapping.setMappedToLatticeField(false);
            }
        }
    }

    private void resolveMetadata(SourceFile sourceFile, FieldMappingDocument fieldMappingDocument, Table table,
            boolean cdlResolve, Table schemaTable, BusinessEntity entity) {
        MetadataResolver resolver = getMetadataResolver(sourceFile, fieldMappingDocument, cdlResolve, schemaTable);

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
        // Set external system column name
        if (BusinessEntity.Account.equals(entity) || BusinessEntity.Contact.equals(entity)) {
            setCDLExternalSystem(resolver.getExternalSystem(), entity);
        }
    }

    private void setCDLExternalSystem(CDLExternalSystem newExternalSystem, BusinessEntity entity) {
        if (newExternalSystem == null ||
                (CollectionUtils.isEmpty(newExternalSystem.getCRMIdList())
                        && CollectionUtils.isEmpty(newExternalSystem.getERPIdList())
                        && CollectionUtils.isEmpty(newExternalSystem.getMAPIdList())
                        && CollectionUtils.isEmpty(newExternalSystem.getOtherIdList()))) {
            return;
        }
        CDLExternalSystem originalSystem = cdlExternalSystemProxy
                .getCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(), entity.name());
        if (originalSystem == null) {
            CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
            cdlExternalSystem.setCRMIdList(newExternalSystem.getCRMIdList());
            cdlExternalSystem.setMAPIdList(newExternalSystem.getMAPIdList());
            cdlExternalSystem.setERPIdList(newExternalSystem.getERPIdList());
            cdlExternalSystem.setOtherIdList(newExternalSystem.getOtherIdList());
            cdlExternalSystem.setIdMapping(newExternalSystem.getIdMapping());
            cdlExternalSystem.setEntity(entity);
            cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(),
                    cdlExternalSystem);
        } else {
            originalSystem.setCRMIdList(mergeList(originalSystem.getCRMIdList(), newExternalSystem.getCRMIdList()));
            originalSystem.setMAPIdList(mergeList(originalSystem.getMAPIdList(), newExternalSystem.getMAPIdList()));
            originalSystem.setERPIdList(mergeList(originalSystem.getERPIdList(), newExternalSystem.getERPIdList()));
            originalSystem.setOtherIdList(mergeList(originalSystem.getOtherIdList(), newExternalSystem.getOtherIdList()));
            originalSystem.addIdMapping(newExternalSystem.getIdMappingList());
            originalSystem.setEntity(originalSystem.getEntity());
            cdlExternalSystemProxy.createOrUpdateCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(),
                    originalSystem);
        }
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, CloseableResourcePool closeableResourcePool,
            String fileDisplayName, boolean checkHeaderFormat, boolean withCDLHeader) {
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
        ValidateFileHeaderUtils.checkForCSVInjectionInFileNameAndHeaders(fileDisplayName, headerFields);
        ValidateFileHeaderUtils.checkForEmptyHeaders(fileDisplayName, headerFields);
        ValidateFileHeaderUtils.checkForLongHeaders(headerFields);
        Collection<String> reservedWords = new ArrayList<>(
                Arrays.asList(ReservedField.Rating.displayName, ReservedField.Percentile.displayName));
        Collection<String> reservedBeginings = new ArrayList<>(
                Arrays.asList(DataCloudConstants.CEAttr, DataCloudConstants.EAttr));
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
            boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
            boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
            Table table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId, enableEntityMatch);
            return table.getAttributes().stream().map(this::getLatticeFieldFromTableAttribute).collect(Collectors.toList());
        } else {
            List<LatticeSchemaField> templateSchemaFields = new ArrayList<>();
            List<Attribute> attributes = dataFeedTask.getImportTemplate().getAttributes();
            for (Attribute accountAttribute : attributes) {
                templateSchemaFields.add(getLatticeFieldFromTableAttribute(accountAttribute));
            }
            templateSchemaFields.forEach(t -> t.setFromExistingTemplate(true));
            return templateSchemaFields;
        }
    }

    private LatticeSchemaField getLatticeFieldFromTableAttribute(Attribute attribute) {
        LatticeSchemaField latticeSchemaField = new LatticeSchemaField();

        latticeSchemaField.setName(attribute.getName());
        // latticeSchemaField.setFieldType(attribute.getPhysicalDataType());
        latticeSchemaField.setFieldType(MetadataResolver.getFieldTypeFromPhysicalType(attribute.getPhysicalDataType()));
        if (UserDefinedType.DATE.equals(latticeSchemaField.getFieldType())) {
            latticeSchemaField.setDateFormatString(attribute.getDateFormatString());
            latticeSchemaField.setTimeFormatString(attribute.getTimeFormatString());
            latticeSchemaField.setTimezone(attribute.getTimezone());
        }
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
        return new MetadataResolver(sourceFile.getPath(), yarnConfiguration, fieldMappingDocument, cdlResolve, null);
    }

    private MetadataResolver getMetadataResolver(SourceFile sourceFile, FieldMappingDocument fieldMappingDocument,
                                                 boolean cdlResolve, Table schemaTable) {
        return new MetadataResolver(sourceFile.getPath(), yarnConfiguration, fieldMappingDocument, cdlResolve, schemaTable);
    }
}
