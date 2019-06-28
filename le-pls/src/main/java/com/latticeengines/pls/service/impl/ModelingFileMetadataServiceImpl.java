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
import java.util.function.Function;
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
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
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
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation.ValidationStatus;
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
            SchemaInterpretation schemaInterpretation, ModelingParameters parameters, boolean isModel, boolean withoutId,
            boolean enableEntityMatch) {
        schemaInterpretation = isModel && enableEntityMatch && schemaInterpretation.equals(SchemaInterpretation.Account) ?
                SchemaInterpretation.ModelAccount : schemaInterpretation;
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
            String systemName = cdlService.getSystemNameFromFeedType(feedType);
            if (StringUtils.isNotEmpty(systemName)) {
                S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                if (s3ImportSystem != null) {
                    for (FieldMapping fieldMapping : fieldMappingFromTemplate.getFieldMappings()) {
                        if (InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())) {
                            if (Boolean.TRUE.equals(s3ImportSystem.isMapToLatticeAccount())) {
                                fieldMapping.setMapToLatticeId(true);
                            }
                        }
                        if (InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField())) {
                            if (Boolean.TRUE.equals(s3ImportSystem.isMapToLatticeContact())) {
                                fieldMapping.setMapToLatticeId(true);
                            }
                        }
                    }
                }
            }
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

    /*
     * validate field mapping document before saving document, two steps: first is comparing field mapping after user changed and
     * mapping with best effort, second step is generating new template table in memory, integrate it to method finalSchemaCheck
     * in DataFeedTaskManage service
     */
    @Override
    public List<FieldValidation> validateFieldMappings(String sourceFileName, FieldMappingDocument fieldMappingDocument,
            String entity, String source, String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);

        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        // modify field document before final validation
        Table templateTable = null;
        if (dataFeedTask != null) {
            templateTable = dataFeedTask.getImportTemplate();
        }

        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                enableEntityMatch);
        MetadataResolver resolver = getMetadataResolver(getSourceFile(sourceFileName), fieldMappingDocument, true,
                standardTable);

        // validate field mapping document
        List<FieldMapping> fieldMappings = fieldMappingDocument.getFieldMappings();
        List<String> ignored = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(fieldMappingDocument.getIgnoredFields())) {
            ignored = fieldMappingDocument.getIgnoredFields();
        }

        List<FieldValidation> validations = new ArrayList<>();

        FieldMappingDocument documentBestEffort = getFieldMappingDocumentBestEffort(sourceFileName, entity, source, feedType);
        // filter off field mapping with cdl external system, or will cause multiple lattice field mapped to the same user field
        Map<String, FieldMapping> userFieldMap = fieldMappings.stream()
                .filter(mapping -> mapping.getCdlExternalSystemType() == null && StringUtils
                        .isNotBlank(mapping.getUserField()))
                .collect(Collectors.toMap(FieldMapping::getUserField, Function.identity()));
        Set<String> standardAttrNames =
                standardTable.getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet());

        // compare field mapping document after being modified with field mapping best effort
        for(FieldMapping bestEffortMapping : documentBestEffort.getFieldMappings()) {
            String userField = bestEffortMapping.getUserField();
            // skip user field mapped to standard attribute or user ignored fields
            if (StringUtils.isNotBlank(userField) && !ignored.contains(userField)) {
                FieldMapping fieldMapping = userFieldMap.get(userField);
                if (fieldMapping == null) {
                    continue;
                }
                if (!standardAttrNames.contains(fieldMapping.getMappedField()) && bestEffortMapping.getFieldType() != fieldMapping.getFieldType()) {
                    String message = String
                            .format("%s is set as %s but appears to only have %s values.", userField, fieldMapping.getFieldType(),
                                    bestEffortMapping.getFieldType());
                    validations.add(createValidation(userField, fieldMapping.getMappedField(), ValidationStatus.WARNING,
                            message));
                } else if (UserDefinedType.DATE.equals(fieldMapping.getFieldType()) && !resolver.checkUserDateType(fieldMapping)) {
                    String userFormat = StringUtils.isBlank(fieldMapping.getTimeFormatString()) ?
                            fieldMapping.getDateFormatString() :
                            fieldMapping.getDateFormatString() + TimeStampConvertUtils.SYSTEM_DELIMITER
                                    + fieldMapping.getTimeFormatString();
                    String correctFormat = StringUtils
                            .isBlank(bestEffortMapping.getTimeFormatString()) ?
                            bestEffortMapping.getDateFormatString() :
                            bestEffortMapping.getDateFormatString() + TimeStampConvertUtils.SYSTEM_DELIMITER
                                    + bestEffortMapping.getTimeFormatString();
                    String message = String
                            .format("%s is set as %s but appears to be %s in your file.", userField,
                                    userFormat, correctFormat);
                    validations.add(createValidation(userField, fieldMapping.getMappedField(), ValidationStatus.WARNING,
                            message));
                }
            }
        }

        List<String> unmappedUserFields = fieldMappings.stream()
                .filter(fieldMapping -> StringUtils.isNotBlank(fieldMapping.getUserField())
                        && StringUtils.isBlank(fieldMapping.getMappedField())).map(fieldMapping -> fieldMapping.getUserField())
                .collect(Collectors.toList());
        Set<String> mappedFields = fieldMappings.stream().map(FieldMapping::getMappedField).filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Table templateWithStandard = mergeTable(templateTable, standardTable);
        Iterator<Attribute> iter = templateWithStandard.getAttributes().iterator();
        // check lattice field both in template and standard table, seek for the case that user field can be mapped, while not
        while (iter.hasNext()) {
            Attribute latticeAttr = iter.next();
            String attrName = latticeAttr.getName();
            if (!mappedFields.contains(attrName)) {
                // check lattice field can be mapped by user field, while not mapped by user
                for (String userField : unmappedUserFields) {
                    if (!ignored.contains(userField)) { // skip if ignored by user
                        if (userField.equals(latticeAttr.getDisplayName())
                                || resolver.isUserFieldMatchWithAttribute(userField, latticeAttr)) {
                            String message = String.format("Lattice field %s can be mapped to %s, while not",
                                    attrName, userField);
                            validations.add(createValidation(null, attrName, ValidationStatus.WARNING, message));
                        }
                    }
                }
            }
        }

        // generate template in memory
        Table generatedTemplate = generateTemplate(sourceFileName, fieldMappingDocument, entity, source, feedType);
        Table finalTemplate = mergeTable(templateTable, generatedTemplate);
        // compare type, require flag between template and standard schema
        checkTemplateTable(finalTemplate, entity, withoutId, enableEntityMatch, validations);

        return validations;
    }

    private Table mergeTable(Table templateTable, Table renderedTable) {
        if (templateTable == null) {
            return renderedTable;
        }
        Map<String, Attribute> templateAttrs = new HashMap<>();
        templateTable.getAttributes().forEach(attribute -> templateAttrs.put(attribute.getName(), attribute));
        for (Attribute attr : renderedTable.getAttributes()) {
            if (!templateAttrs.containsKey(attr.getName())) {
                templateTable.addAttribute(attr);
            }
        }
        return templateTable;
    }

    private FieldValidation createValidation(String userField, String latticeField, ValidationStatus status,
            String message) {
        FieldValidation validation = new FieldValidation();
        validation.setUserField(userField);
        validation.setLatticeField(latticeField);
        validation.setStatus(status);
        validation.setMessage(message);
        return validation;
    }

    void checkTemplateTable(Table finalTemplate, String entity, boolean withoutId, boolean enableEntityMatch, List<FieldValidation> validations) {
        Map<String, Attribute> standardAttrs = new HashMap<>();
        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId
                , enableEntityMatch);
        standardTable.getAttributes().forEach(attribute -> standardAttrs.put(attribute.getName(), attribute));
        Map<String, Attribute> templateAttrs = new HashMap<>();
        finalTemplate.getAttributes().forEach(attribute -> templateAttrs.put(attribute.getName(), attribute));
        for (Map.Entry<String, Attribute> attrEntry : standardAttrs.entrySet()) {
            if (attrEntry.getValue().getRequired() && attrEntry.getValue().getDefaultValueStr() == null) {
                if(!templateAttrs.containsKey(attrEntry.getKey())) {
                    String message = String
                            .format("%s is not mapped, and is a required field.", attrEntry.getKey());
                    validations.add(createValidation(null, attrEntry.getKey(), ValidationStatus.ERROR, message));
                }
            }
            if (templateAttrs.containsKey(attrEntry.getKey())) {
                Attribute attr1 = attrEntry.getValue();
                Attribute attr2 = templateAttrs.get(attrEntry.getKey());
                if (!attr1.getPhysicalDataType().equalsIgnoreCase(attr2.getPhysicalDataType())) {
                        String message = "Data type is not the same for attribute: " + attr1.getDisplayName();
                        validations.add(createValidation(attr2.getDisplayName(), attr2.getName(),
                                ValidationStatus.ERROR, message));
                }
                if (!attr1.getRequired().equals(attr2.getRequired())) {
                    String message = "Required flag is not the same for attribute: " + attr1.getDisplayName();
                    validations.add(createValidation(attr2.getDisplayName(), attr2.getName(),
                            ValidationStatus.ERROR, message));
                }
            }
        }
    }

    @Override
    public void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument, boolean isModel,
            boolean enableEntityMatch) {
        decodeFieldMapping(fieldMappingDocument);
        SourceFile sourceFile = getSourceFile(sourceFileName);
        SchemaInterpretation schemaInterpretation = sourceFile.getSchemaInterpretation();
        schemaInterpretation = enableEntityMatch && isModel && schemaInterpretation.equals(SchemaInterpretation.Account) ? SchemaInterpretation.ModelAccount : schemaInterpretation;
        Table table = getTableFromParameters(schemaInterpretation, false, enableEntityMatch);
        resolveMetadata(sourceFile, fieldMappingDocument, table, false, null, null);
    }

    private Table generateTemplate(String sourceFileName, FieldMappingDocument fieldMappingDocument, String entity,
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
        MetadataResolver resolver = getMetadataResolver(sourceFile, fieldMappingDocument, true, schemaTable);

        log.info(String.format("the ignored fields are: %s", fieldMappingDocument.getIgnoredFields()));
        if (!resolver.isFieldMappingDocumentFullyDefined()) {
            throw new RuntimeException(
                    String.format("Metadata is not fully defined for file %s", sourceFile.getName()));
        }
        resolver.calculateBasedOnFieldMappingDocument(table);
        return resolver.getMetadata();
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
        // 1. set system related mapping //only apply to Account / Contact
        if (BusinessEntity.Account.equals(entity) || BusinessEntity.Contact.equals(entity)) {
            List<FieldMapping> customerLatticeIdList = new ArrayList<>();
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
                                    if (importSystem.isMapToLatticeAccount()) {
                                        FieldMapping customerLatticeId = new FieldMapping();
                                        customerLatticeId.setUserField(fieldMapping.getUserField());
                                        customerLatticeId.setMappedField(InterfaceName.CustomerAccountId.name());
                                        customerLatticeId.setFieldType(fieldMapping.getFieldType());
                                        customerLatticeIdList.add(customerLatticeId);
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
                                    if (importSystem.isMapToLatticeContact()) {
                                        FieldMapping customerLatticeId = new FieldMapping();
                                        customerLatticeId.setUserField(fieldMapping.getUserField());
                                        customerLatticeId.setMappedField(InterfaceName.CustomerContactId.name());
                                        customerLatticeId.setFieldType(fieldMapping.getFieldType());
                                        customerLatticeIdList.add(customerLatticeId);
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
                                        FieldMapping customerLatticeId = new FieldMapping();
                                        customerLatticeId.setUserField(fieldMapping.getUserField());
                                        customerLatticeId.setMappedField(InterfaceName.CustomerAccountId.name());
                                        customerLatticeId.setFieldType(fieldMapping.getFieldType());
                                        customerLatticeIdList.add(customerLatticeId);
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
                                        FieldMapping customerLatticeId = new FieldMapping();
                                        customerLatticeId.setUserField(fieldMapping.getUserField());
                                        customerLatticeId.setMappedField(InterfaceName.CustomerContactId.name());
                                        customerLatticeId.setFieldType(fieldMapping.getFieldType());
                                        customerLatticeIdList.add(customerLatticeId);
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
            if (CollectionUtils.isNotEmpty(customerLatticeIdList)) {
                boolean customerAccountExists = false;
                boolean customerContactExists = false;
                for (FieldMapping customerLatticeId : customerLatticeIdList) {
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
                    if (InterfaceName.CustomerAccountId.name().equals(customerLatticeId.getMappedField())) {
                        customerAccountExists = true;
                    } else if (InterfaceName.CustomerContactId.name().equals(customerLatticeId.getMappedField())) {
                        customerContactExists = true;
                    }
                }
                if (!customerAccountExists) {
                    fieldMappingDocument.getFieldMappings()
                            .removeIf(fieldMapping -> InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField()));
                }
                if (!customerContactExists) {
                    fieldMappingDocument.getFieldMappings()
                            .removeIf(fieldMapping -> InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField()));
                }

            } else {
                fieldMappingDocument.getFieldMappings().removeIf(fieldMapping ->
                        InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField())
                                || InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField()));
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
