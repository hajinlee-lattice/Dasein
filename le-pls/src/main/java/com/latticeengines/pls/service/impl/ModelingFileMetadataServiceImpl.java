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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.app.exposed.service.impl.CommonTenantConfigServiceImpl;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.InputValidatorWrapper;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.AttributeLengthValidator;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.TemplateValidator;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SchemaInterpretationFunctionalInterface;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourcefileConfig;
import com.latticeengines.domain.exposed.pls.UniqueIdentifierConfig;
import com.latticeengines.domain.exposed.pls.frontend.ExtraFieldMappingInfo;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation.ValidationStatus;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationResult;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.RequiredType;
import com.latticeengines.domain.exposed.pls.frontend.ValidationCategory;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.validation.ReservedField;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.CDLExternalSystemUtils;
import com.latticeengines.pls.util.EntityMatchGAConverterUtils;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("modelingFileMetadataService")
public class ModelingFileMetadataServiceImpl implements ModelingFileMetadataService {
    private static final Logger log = LoggerFactory.getLogger(ModelingFileMetadataServiceImpl.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private CDLService cdlService;

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Inject
    private CommonTenantConfigServiceImpl appTenantConfigService;

    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Override
    public FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName,
                                                                  SchemaInterpretation schemaInterpretation, ModelingParameters parameters,
                                                                  boolean isModel, boolean withoutId,
                                                                  boolean enableEntityMatch, boolean onlyGA) {
        schemaInterpretation = isModel && enableEntityMatch && schemaInterpretation.equals(SchemaInterpretation.Account) ?
                SchemaInterpretation.ModelAccount : schemaInterpretation;
        SourceFile sourceFile = getSourceFile(sourceFileName);
        log.info("SchemaInterpretation=" + schemaInterpretation + " isModel=" + isModel + " enableEntityMatch="
                + enableEntityMatch);
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }

        MetadataResolver resolver = getMetadataResolver(sourceFile, null, false);
        Table table = getTableFromParameters(sourceFile.getSchemaInterpretation(), withoutId, enableEntityMatch, onlyGA);
        return resolver.getFieldMappingsDocumentBestEffort(table);
    }

    @Override
    public FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName, String entity, String source,
                                                                  String feedType) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        log.info(String.format("Customer Space: %s, entity: %s, source: %s, datafeed: %s", customerSpace.toString(),
                entity, source, feedType));
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        String systemName = EntityTypeUtils.getSystemName(feedType);
        EntityType entityType = EntityTypeUtils.matchFeedType(feedType);
        SchemaInterpretation schemaInterpretation;
        Table table;
        S3ImportSystem s3ImportSystem = null;
        Set<String> systemIdSet = cdlService.getAllS3ImportSystemIdSet(customerSpace.toString());
        if (StringUtils.isNotEmpty(systemName) && entityType != null) {
            s3ImportSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
            schemaInterpretation = entityType.getSchemaInterpretation();
            table = SchemaRepository.instance().getSchema(s3ImportSystem.getSystemType(), entityType,
                    batonService.isEntityMatchEnabled(customerSpace),
                    batonService.onlyEntityMatchGAEnabled(customerSpace));

        } else {
            schemaInterpretation = SchemaInterpretation.getByName(entity);
            boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
            table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                    batonService.isEntityMatchEnabled(customerSpace),
                    batonService.onlyEntityMatchGAEnabled(customerSpace));
        }
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        FieldMappingDocument fieldMappingFromSchemaRepo = resolver.getFieldMappingsDocumentBestEffort(table);
        generateExtraFieldMappingInfo(fieldMappingFromSchemaRepo, true, systemIdSet);
        FieldMappingDocument resultDocument;
        if (dataFeedTask == null) {
            resultDocument = fieldMappingFromSchemaRepo;
        } else {
            Table templateTable = dataFeedTask.getImportTemplate();
            FieldMappingDocument fieldMappingFromTemplate = getFieldMappingBaseOnTable(sourceFile, templateTable);
            if (StringUtils.isNotEmpty(systemName)) {
                s3ImportSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                if (s3ImportSystem != null) {
                    for (FieldMapping fieldMapping : fieldMappingFromTemplate.getFieldMappings()) {
                        if (StringUtils.isNotEmpty(s3ImportSystem.getAccountSystemId())
                                && s3ImportSystem.getAccountSystemId().equals(fieldMapping.getMappedField())) {
                            if (Boolean.TRUE.equals(s3ImportSystem.isMapToLatticeAccount())) {
                                fieldMapping.setMapToLatticeId(true);
                            }
                        }
                        if (StringUtils.isNotEmpty(s3ImportSystem.getContactSystemId())
                                && s3ImportSystem.getContactSystemId().equals(fieldMapping.getMappedField())) {
                            if (Boolean.TRUE.equals(s3ImportSystem.isMapToLatticeContact())) {
                                fieldMapping.setMapToLatticeId(true);
                            }
                        }
                    }
                }
                setSystemMapping(customerSpace.toString(), systemName, fieldMappingFromTemplate);
            }
            updateDocumentForDataFeedConfig(dataFeedTask, entity, fieldMappingFromTemplate);
            resultDocument = mergeFieldMappingBestEffort(fieldMappingFromTemplate, fieldMappingFromSchemaRepo,
                    templateTable, table, systemIdSet);
        }
        EntityMatchGAConverterUtils.convertGuessingMappings(enableEntityMatch, enableEntityMatchGA, resultDocument,
                s3ImportSystem);
        return resultDocument;
    }

    private void updateDocumentForDataFeedConfig(DataFeedTask dataFeedTask, String entity,
                                                 FieldMappingDocument document) {
        if (StringUtils.isEmpty(entity)) {
            return;
        }
        BusinessEntity businessEntity = BusinessEntity.getByName(entity);
        if ((BusinessEntity.Account.equals(businessEntity) || BusinessEntity.Contact.equals(businessEntity))
                && dataFeedTask != null && dataFeedTask.getDataFeedTaskConfig() != null && dataFeedTask.getDataFeedTaskConfig().getTemplateValidators() != null) {
            for (TemplateValidator validator : dataFeedTask.getDataFeedTaskConfig().getTemplateValidators()) {
                if (validator instanceof AttributeLengthValidator) {
                    AttributeLengthValidator lengthValidator = (AttributeLengthValidator) validator;
                    if (checkAttributeName(lengthValidator.getAttributeName(), businessEntity)) {
                        for (FieldMapping fieldMapping : document.getFieldMappings()) {
                            if (lengthValidator.getAttributeName().equals(fieldMapping.getMappedField())) {
                                fieldMapping.setLength(lengthValidator.getLength());
                                fieldMapping.setRequired(!lengthValidator.getNullable());
                                return;
                            }
                        }
                        return;
                    }
                }
            }
        }
    }

    private boolean checkAttributeName(String name, BusinessEntity entity) {
        if (StringUtils.isEmpty(name)) {
            return false;
        } else if (BusinessEntity.Account.equals(entity)) {
            return name.contains("AccountId");
        } else if (BusinessEntity.Contact.equals(entity)) {
            return name.contains("ContactId");
        }
        return false;
    }

    private void setSystemMapping(String customerSpace,
                                  String currentSystem, FieldMappingDocument fieldMappingDocument) {
        List<S3ImportSystem> allImportSystem = cdlService.getAllS3ImportSystem(customerSpace);
        if (CollectionUtils.isEmpty(allImportSystem)) {
            return;
        }
        Map<String, S3ImportSystem> accountSystemIdMap =
                allImportSystem.stream()
                        .filter(s3ImportSystem -> StringUtils.isNotEmpty(s3ImportSystem.getAccountSystemId()))
                        .collect(Collectors.toMap(S3ImportSystem::getAccountSystemId, s3ImportSystem -> s3ImportSystem));

        Map<String, S3ImportSystem> contactSystemIdMap =
                allImportSystem.stream()
                        .filter(s3ImportSystem -> StringUtils.isNotEmpty(s3ImportSystem.getContactSystemId()))
                        .collect(Collectors.toMap(S3ImportSystem::getContactSystemId, s3ImportSystem -> s3ImportSystem));

        Map<String, S3ImportSystem> leadSystemIdMap = allImportSystem.stream()
                .filter(s3ImportSystem -> StringUtils.isNotEmpty(s3ImportSystem.getSecondaryContactId(EntityType.Leads)))
                .collect(Collectors.toMap(s3ImportSystem -> s3ImportSystem.getSecondaryContactId(EntityType.Leads),
                        s3ImportSystem -> s3ImportSystem));

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (StringUtils.isNotEmpty(fieldMapping.getMappedField())) {
                if (accountSystemIdMap.containsKey(fieldMapping.getMappedField())) {
                    fieldMapping.setSystemName(accountSystemIdMap.get(fieldMapping.getMappedField()).getName());
                    fieldMapping.setIdType(FieldMapping.IdType.Account);
                    fieldMapping.setMappedToLatticeSystem(true);
                } else if (contactSystemIdMap.containsKey(fieldMapping.getMappedField())) {
                    fieldMapping.setSystemName(contactSystemIdMap.get(fieldMapping.getMappedField()).getName());
                    fieldMapping.setIdType(FieldMapping.IdType.Contact);
                    fieldMapping.setMappedToLatticeSystem(true);
                } else if (leadSystemIdMap.containsKey(fieldMapping.getMappedField())) {
                    fieldMapping.setSystemName(leadSystemIdMap.get(fieldMapping.getMappedField()).getName());
                    fieldMapping.setIdType(FieldMapping.IdType.Lead);
                    fieldMapping.setMappedToLatticeSystem(true);
                }
            }

        }
    }

    private void generateExtraFieldMappingInfo(FieldMappingDocument fieldMappingDocument, boolean standard,
                                               Set<String> systemIds) {
        if (fieldMappingDocument == null || CollectionUtils.isEmpty(fieldMappingDocument.getFieldMappings())) {
            return;
        }
        ExtraFieldMappingInfo extraFieldMappingInfo = new ExtraFieldMappingInfo();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (!standard && fieldMapping.isMappedToLatticeField()
                    && StringUtils.isNotEmpty(fieldMapping.getMappedField())
                    && !systemIds.contains(fieldMapping.getMappedField())) {
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
                                                             FieldMappingDocument standardMapping,
                                                             Table templateTable, Table standardTable,
                                                             Set<String> systemIds) {
        // Create schema user -> fieldMapping
        Map<String, FieldMapping> standardMappingMap = new HashMap<>();
        for (FieldMapping fieldMapping : standardMapping.getFieldMappings()) {
            standardMappingMap.put(fieldMapping.getUserField(), fieldMapping);
        }
        Set<String> alreadyMappedField = templateMapping.getFieldMappings().stream()
                .map(FieldMapping::getMappedField)
                .filter(Objects::nonNull).collect(Collectors.toSet());
        // Add mapped fields from schema to template, if not already in a template mapped field
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
        generateExtraFieldMappingInfo(templateMapping, false, systemIds);
        // filter existingMappings
        Map<String, Attribute> standardAttrs =
                standardTable.getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));
        // Take out extra fields that are also in the schema table.
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
            if (!mappedAttr.contains(key) && !standardAttrs.containsKey(key) && !systemIds.contains(key)) {
                missedAttrs.put(key, value);
            }
        });
        missedAttrs.forEach((key, value) -> {
            FieldMapping fieldMapping = new FieldMapping();
            fieldMapping.setUserField(
                    value.getSourceAttrName() == null ? value.getDisplayName() : value.getSourceAttrName());
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
                                         boolean enableEntityMatch, boolean onlyGA) {
        Table table = SchemaRepository.instance().getSchema(schemaInterpretation, withoutId, enableEntityMatch, onlyGA);
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
    public FieldValidationResult validateFieldMappings(String sourceFileName, FieldMappingDocument fieldMappingDocument,
                                                       String entity, String source, String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);

        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);
        // modify field document before final validation
        Table templateTable = null;
        String dataFeedTaskUniqueId = null;
        if (dataFeedTask != null) {
            templateTable = dataFeedTask.getImportTemplate();
            dataFeedTaskUniqueId = dataFeedTask.getUniqueId();
        }

        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                enableEntityMatch || enableEntityMatchGA, enableEntityMatchGA);
        MetadataResolver resolver = getMetadataResolver(getSourceFile(sourceFileName), fieldMappingDocument, true,
                standardTable);

        // validate field mapping document
        List<FieldMapping> fieldMappings = fieldMappingDocument.getFieldMappings();
        List<String> ignored = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(fieldMappingDocument.getIgnoredFields())) {
            ignored = fieldMappingDocument.getIgnoredFields();
        }

        List<FieldValidation> validations = new ArrayList<>();
        Map<ValidationCategory, List<FieldValidation>> groupedValidations =
                initializeGroupedValidation();


        FieldMappingDocument documentBestEffort = getFieldMappingDocumentBestEffort(sourceFileName, entity, source, feedType);
        // multiple lattice field mapped to the same user field, the value of map should be a list
        Map<String, List<FieldMapping>> userFieldMap = fieldMappings.stream()
                .filter(mapping -> StringUtils.isNotBlank(mapping.getUserField()))
                .collect(Collectors.groupingBy(FieldMapping::getUserField));
        Set<String> standardAttrNames =
                standardTable.getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet());

        // 1. check user field to standard field 2. check user field to matched system
        checkMultipleUserFieldsMappedToLatticeField(fieldMappings, standardAttrNames, groupedValidations, validations);

        compareStandardFields(templateTable, fieldMappingDocument, standardTable, validations, groupedValidations,
                customerSpace,
                enableEntityMatch);
        // compare field mapping document after being modified with field mapping best effort
        for (FieldMapping bestEffortMapping : documentBestEffort.getFieldMappings()) {
            String userField = bestEffortMapping.getUserField();
            // skip checking type for special mapping for entity match
            if (bestEffortMapping.getIdType() != null) {
                continue;
            }
            // skip user field mapped to standard attribute or user ignored fields
            if (StringUtils.isNotBlank(userField) && !ignored.contains(userField)) {
                List<FieldMapping> fieldMappingGroup = userFieldMap.get(userField);
                if (CollectionUtils.isEmpty(fieldMappingGroup)) {
                    continue;
                }
                for (FieldMapping fieldMapping : fieldMappingGroup) {
                    if (fieldMapping.getIdType() != null) {
                        continue;
                    }
                    if (!standardAttrNames.contains(fieldMapping.getMappedField()) && bestEffortMapping.getFieldType() != fieldMapping.getFieldType()) {
                        String message = String
                                .format("%s is set as %s but appears to only have %s values.", userField, fieldMapping.getFieldType(),
                                        bestEffortMapping.getFieldType());
                        FieldValidation validation = createValidation(userField, fieldMapping.getMappedField(),
                                ValidationStatus.WARNING,
                                message);
                        validations.add(validation);
                        groupedValidations.get(ValidationCategory.DataType).add(validation);
                    } else if (UserDefinedType.DATE.equals(fieldMapping.getFieldType())) {
                        String userFormat = StringUtils.isBlank(fieldMapping.getTimeFormatString()) ?
                                fieldMapping.getDateFormatString() :
                                fieldMapping.getDateFormatString() + TimeStampConvertUtils.SYSTEM_DELIMITER
                                        + fieldMapping.getTimeFormatString();
                        String formatWithBestEffort = StringUtils
                                .isBlank(bestEffortMapping.getTimeFormatString()) ?
                                bestEffortMapping.getDateFormatString() :
                                bestEffortMapping.getDateFormatString() + TimeStampConvertUtils.SYSTEM_DELIMITER
                                        + bestEffortMapping.getTimeFormatString();

                        // deal with case format can't parse the value
                        StringBuilder warningMessage = new StringBuilder();
                        boolean match = resolver.checkUserDateType(userField, fieldMapping.getDateFormatString(),
                                fieldMapping.getTimeFormatString(), fieldMapping.getTimezone(), warningMessage,
                                formatWithBestEffort);
                        if (!match && warningMessage.length() > 0) {
                            FieldValidation validation = createValidation(userField, fieldMapping.getMappedField(),
                                    ValidationStatus.WARNING, warningMessage.toString());
                            validations.add(validation);
                            groupedValidations.get(ValidationCategory.DateFormat).add(validation);
                        } else if (StringUtils.isNotBlank(userFormat) && !userFormat.equals(formatWithBestEffort)) {
                            // this is case that user change the date/time format which can be parsed
                            String message =  String.format("%s is set as %s which can parse the value from uploaded " +
                                    "file.", userField, userFormat);
                            FieldValidation validation = createValidation(userField, fieldMapping.getMappedField(),
                                    ValidationStatus.WARNING, message);
                            validations.add(validation);
                            groupedValidations.get(ValidationCategory.DateFormat).add(validation);
                        }
                    }
                }
            }
        }

        List<String> unmappedUserFields = fieldMappings.stream()
                .filter(fieldMapping -> StringUtils.isNotBlank(fieldMapping.getUserField())
                        && StringUtils.isBlank(fieldMapping.getMappedField())).map(FieldMapping::getUserField)
                .collect(Collectors.toList());
        Set<String> mappedFields = fieldMappings.stream().map(FieldMapping::getMappedField).filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Table templateWithStandard = mergeTable(templateTable, standardTable);
        // check lattice field both in template and standard table, seek for the case that user field can be mapped, while not
        for (Attribute latticeAttr : templateWithStandard.getAttributes()) {
            String attrName = latticeAttr.getName();
            if (!mappedFields.contains(attrName)) {
                // check lattice field can be mapped by user field, while not mapped by user
                for (String userField : unmappedUserFields) {
                    if (!ignored.contains(userField)) { // skip if ignored by user
                        if (userField.equals(latticeAttr.getSourceAttrName() == null ? latticeAttr.getDisplayName() :
                                latticeAttr.getSourceAttrName())
                                || resolver.isUserFieldMatchWithAttribute(userField, latticeAttr)) {
                            String message = String.format("%s is currently unmapped and can be mapped to Standard " +
                                    " Field %s.", userField, attrName);
                            FieldValidation validation = createValidation(null, attrName, ValidationStatus.WARNING,
                                    message);
                            validations.add(validation);
                            groupedValidations.get(ValidationCategory.ColumnMapping).add(validation);
                        }
                    }
                }
            }
        }
        // generate template in memory
        Table generatedTemplate = generateTemplate(sourceFileName, fieldMappingDocument, entity, source, feedType);
        FieldValidationResult fieldValidationResult = new FieldValidationResult();
        int limit;
        if (BusinessEntity.Account.name().equals(entity)) {
            limit = appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(MultiTenantContext.getShortTenantId(), DataLicense.ACCOUNT.getDataLicense());
            validateFieldSize(fieldValidationResult, customerSpace, entity, generatedTemplate, limit, enableEntityMatch, enableEntityMatchGA);
        } else if (BusinessEntity.Contact.name().equals(entity)) {
            limit = appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(MultiTenantContext.getShortTenantId(), DataLicense.CONTACT.getDataLicense());
            validateFieldSize(fieldValidationResult, customerSpace, entity, generatedTemplate, limit, enableEntityMatch, enableEntityMatchGA);
        }
        int otherIdLimit =
                appTenantConfigService.getOtherIdQuotaLimit(MultiTenantContext.getShortTenantId());
        validateOtherIdSize(fieldValidationResult, fieldMappingDocument, otherIdLimit);
        Table finalTemplate = mergeTable(templateTable, generatedTemplate);
        // compare type, require flag between template and standard schema
        checkTemplateTable(finalTemplate, entity, withoutId, enableEntityMatch || enableEntityMatchGA,
                enableEntityMatchGA, validations, groupedValidations);
        crosscheckDataType(customerSpace, entity, source, finalTemplate, dataFeedTaskUniqueId, validations, groupedValidations);
        fieldValidationResult.setFieldValidations(validations);
        fieldValidationResult.setGroupedValidations(groupedValidations);
        fieldValidationResult.setValidationMessages(
                Stream.of(ValidationCategory.values())
                        .filter(e -> CollectionUtils.isEmpty(groupedValidations.get(e)))
                        .collect(Collectors.toMap(e -> e, ValidationCategory::getValidMessage)));
        return fieldValidationResult;
    }

    private void checkMultipleUserFieldsMappedToLatticeField(List<FieldMapping> fieldMappings,
                                                              Set<String> standardAttrNames,
                                                              Map<ValidationCategory, List<FieldValidation>> groupedValidations,
                                                              List<FieldValidation> validations) {
        Set<String> mappedStandardFields = new HashSet<>();
        Set<String> mappingKeys = new HashSet<>();
        for (FieldMapping fieldMapping : fieldMappings) {
            // check if there's multiple mapping to standard field
            if (fieldMapping.getMappedField() != null) {
                if (standardAttrNames.contains(fieldMapping.getMappedField())) {
                    if (mappedStandardFields.contains(fieldMapping.getMappedField())) {
                        String message =
                                "Multiple user fields are mapped to standard field " + fieldMapping.getMappedField();
                        FieldValidation validation = createValidation(fieldMapping.getUserField(),
                                fieldMapping.getMappedField(),
                                ValidationStatus.ERROR, message);
                        validations.add(validation);
                        groupedValidations.get(ValidationCategory.ColumnMapping).add(validation);
                    } else {
                        mappedStandardFields.add(fieldMapping.getMappedField());
                    }
                }
            }
            // check if multiple user fields mapped to system
            if (fieldMapping.getIdType() != null && StringUtils.isNotBlank(fieldMapping.getSystemName())) {
                String key = fieldMapping.getIdType() + "|" + fieldMapping.getSystemName();
                if (mappingKeys.contains(key)) {
//                    String message = String.format(
//                            "Multiple user fields are mapped to %s with the same type %s",
//                            fieldMapping.getSystemName(), fieldMapping.getIdType());
//                    FieldValidation validation = createValidation(fieldMapping.getUserField(),
//                            fieldMapping.getMappedField(),
//                            ValidationStatus.ERROR, message);
//                    validations.add(validation);
//                    groupedValidations.get(ValidationCategory.ColumnMapping).add(validation);
                } else {
                    mappingKeys.add(key);
                }

            }
            // check if length is valid
            if (!fieldMapping.checkLengthValid()) {
                String message = String.format("The length of unique ID %d is invalid", fieldMapping.getLength());
                FieldValidation validation = createValidation(null, null, ValidationStatus.ERROR, message);
                validations.add(validation);
                groupedValidations.get(ValidationCategory.Others).add(validation);
            }
        }
    }

    private void compareStandardFields(Table templateTable,
                                       FieldMappingDocument fieldMappingDocument,
                                       Table standardTable,
                                       List<FieldValidation> validations,
                                       Map<ValidationCategory, List<FieldValidation>> groupedValidations,
                                       CustomerSpace customerSpace,
                                       boolean entityMatchEnabled) {
        if (templateTable == null) {
            return;
        }
        if (!entityMatchEnabled) {
            return ;
        }
        Set<String> standardAttrNames =
                standardTable.getAttributes()
                        .stream()
                        .filter(attr -> attr.getDefaultValueStr() == null)
                        .map(Attribute::getName)
                        .collect(Collectors.toSet());
        Set<String> allImportSystemIds = cdlService.getAllS3ImportSystemIdSet(customerSpace.toString());
        if (CollectionUtils.isNotEmpty(allImportSystemIds)) {
            standardAttrNames.addAll(allImportSystemIds);
        }
        Map<String, String> previousStandardFieldMapping =
                templateTable.getAttributes()
                        .stream()
                        .filter(attr -> standardAttrNames.contains(attr.getName()))
                        .collect(Collectors.toMap(Attribute::getName,
                                attr -> StringUtils.isNotBlank(attr.getSourceAttrName()) ? attr.getSourceAttrName() : attr.getDisplayName()));
        for (FieldMapping mapping : fieldMappingDocument.getFieldMappings()) {
            if (standardAttrNames.contains(mapping.getMappedField())) {
                String preUserField = previousStandardFieldMapping.get(mapping.getMappedField());
                String userField = mapping.getUserField();
                if (StringUtils.isNotBlank(preUserField) && StringUtils.isBlank(userField) ) {
                    String message = String.format("%s was previously mapped to %s. " +
                                    "This mapping can be changed, but removing the mapping is currently not supported.",
                            mapping.getMappedField(), preUserField);
                    FieldValidation validation = createValidation(userField, mapping.getMappedField(),
                            ValidationStatus.WARNING, message);
                    validations.add(validation);
                    groupedValidations.get(ValidationCategory.ColumnMapping).add(validation);
                } else if (StringUtils.isNotBlank(preUserField) && StringUtils.isNotBlank(userField) &&
                        !preUserField.equals(userField)) {
                    String message = String.format("standard field mapping changed from %s -> %s to %s -> %s.",
                            preUserField, mapping.getMappedField(), userField, mapping.getMappedField());
                    FieldValidation validation = createValidation(userField, mapping.getMappedField(),
                            ValidationStatus.WARNING, message);
                    validations.add(validation);
                    groupedValidations.get(ValidationCategory.ColumnMapping).add(validation);
                }
                previousStandardFieldMapping.remove(mapping.getMappedField());
            }
        }
        previousStandardFieldMapping.forEach((mappedField, userField) -> {
            if (StringUtils.isNotBlank(userField)) {
                String message = String.format("%s was previously mapped to %s. " +
                                "This mapping can be changed, but removing the mapping is currently not supported.",
                        mappedField, userField);
                FieldValidation validation = createValidation(userField, mappedField, ValidationStatus.WARNING,
                        message);
                validations.add(validation);
                groupedValidations.get(ValidationCategory.ColumnMapping).add(validation);
            }
        });
    }

    private void validateFieldSize(FieldValidationResult fieldValidationResult, CustomerSpace customerSpace, String entity, Table generatedTemplate,
                                   int limit, boolean enableEntityMatch, boolean enableEntityMatchGA) {
        Set<String> attributes = new HashSet<>();
        List<DataFeedTask> dataFeedTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace.toString(), entity);
        if (CollectionUtils.isNotEmpty(dataFeedTasks)) {
            for (DataFeedTask dataFeedTask : dataFeedTasks) {
                Table table = dataFeedTask.getImportTemplate();
                if (table != null) {
                    attributes.addAll(table.getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet()));
                }
            }
        }
        AttrConfigRequest configRequest = cdlAttrConfigProxy.getAttrConfigByEntity(customerSpace.toString(), BusinessEntity.getByName(entity), true);
        Set<String> inactiveNames = configRequest.getAttrConfigs().stream().filter(config -> !AttrState.Active.equals(config.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class)))
                .map(AttrConfig::getAttrName).collect(Collectors.toSet());
        // needs to convert AccountId or ContactId mapping
        boolean convertName = enableEntityMatchGA && !enableEntityMatch;
        for (Attribute attribute : generatedTemplate.getAttributes()) {
            if (convertName) {
                if (InterfaceName.AccountId.name().equals(attribute.getName())) {
                    attributes.add(InterfaceName.CustomerAccountId.name());
                } else if (InterfaceName.ContactId.name().equals(attribute.getName())) {
                    attributes.add(InterfaceName.CustomerContactId.name());
                } else {
                    attributes.add(attribute.getName());
                }
            } else {
                attributes.add(attribute.getName());
            }
        }
        attributes = attributes.stream().filter(name -> !inactiveNames.contains(name)).collect(Collectors.toSet());
        int fieldSize = attributes.size();
        ValidateFileHeaderUtils.exceedQuotaFieldSize(fieldValidationResult, fieldSize, limit, entity);
    }

    private void validateOtherIdSize(FieldValidationResult fieldValidationResult,
                                     FieldMappingDocument fieldMappingDocument, int otherIdLimit) {
        Map<CDLExternalSystemType, Integer> sizeMap = new HashMap<>();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getCdlExternalSystemType() != null) {
                sizeMap.putIfAbsent(fieldMapping.getCdlExternalSystemType(), 0);
                sizeMap.put(fieldMapping.getCdlExternalSystemType(),
                        sizeMap.get(fieldMapping.getCdlExternalSystemType()) + 1);
            }
        }
        if (MapUtils.isNotEmpty(sizeMap)) {
            for (Map.Entry<CDLExternalSystemType, Integer> entry : sizeMap.entrySet()) {
                CDLExternalSystemType type = entry.getKey();
                Integer fieldSize = entry.getValue();
                ValidateFileHeaderUtils.exceedQuotaFieldSize(fieldValidationResult, fieldSize, otherIdLimit, type.name());
            }
        }
    }

    private void crosscheckDataType(CustomerSpace customerSpace, String entity, String source, Table metaTable,
                                    String dataFeedTaskUniqueId, List<FieldValidation> validations,
                                    Map<ValidationCategory, List<FieldValidation>> groupedValidations) {
        List<DataFeedTask> dataFeedTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace.toString(),
                entity);
        if (dataFeedTasks != null && dataFeedTasks.size() != 0) {
            boolean updatedAttrName = false;
            for (DataFeedTask dataFeedTask : dataFeedTasks) {
                if (!updatedAttrName) {
                    updateTableAttributeName(dataFeedTask.getImportTemplate(), metaTable);
                    updatedAttrName = true;
                }
                if (StringUtils.equals(dataFeedTask.getUniqueId(), dataFeedTaskUniqueId)) {
                    continue;
                }
                List<String> inconsistentAttrs = compareAttribute(dataFeedTask.getSource(),
                        dataFeedTask.getImportTemplate(), source, metaTable);
                if (CollectionUtils.isNotEmpty(inconsistentAttrs)) {
                    String message = String.format(
                            "The following field data type is not consistent with the one that already exists: %s",
                            String.join(",", inconsistentAttrs));
                    FieldValidation validation = createValidation(null, null,
                            ValidationStatus.ERROR, message);
                    validations.add(validation);
                    groupedValidations.get(ValidationCategory.DataType).add(validation);
                }
            }
        }
    }

    void updateTableAttributeName(Table templateTable, Table metaTable) {
        Map<String, Attribute> templateAttrs = new HashMap<>();
        templateTable.getAttributes()
                .forEach(attribute -> templateAttrs.put(attribute.getName().toLowerCase(), attribute));
        for (Attribute attr : metaTable.getAttributes()) {
            if (templateAttrs.containsKey(attr.getName().toLowerCase())) {
                attr.setName(templateAttrs.get(attr.getName().toLowerCase()).getName());
            }
        }
    }
    private List<String> compareAttribute(String baseSource, Table baseTable, String targetSource, Table targetTable) {
        List<String> inconsistentAttrs = new ArrayList<>();
        Map<String, Attribute> baseAttrs = new HashMap<>();
        baseTable.getAttributes().forEach(attribute -> baseAttrs.put(attribute.getName().toLowerCase(), attribute));
        for (Attribute attr : targetTable.getAttributes()) {
            if (baseAttrs.containsKey(attr.getName().toLowerCase())) {
                Schema.Type baseType = getAvroType(baseAttrs.get(attr.getName().toLowerCase()), baseSource);
                Schema.Type targetType = getAvroType(attr, targetSource);
                if (baseType != targetType) {
                    inconsistentAttrs.add(attr.getName());
                }
            }
        }
        return inconsistentAttrs;
    }

    public Schema.Type getAvroType(Attribute attribute, String source) {
        if (SourceType.VISIDB.getName().toLowerCase().equals(source.toLowerCase())) {
            if (attribute == null) {
                return null;
            } else {
                return Schema.Type.valueOf(attribute.getPhysicalDataType().toUpperCase());
            }
        } else if (SourceType.FILE.getName().toLowerCase().equals(source.toLowerCase())) {
            Schema.Type type = null;
            if (attribute.getPhysicalDataType() == null) {
                throw new RuntimeException(
                        String.format("Physical data type for attribute %s is null", attribute.getName()));
            }
            String typeStrLowerCase = attribute.getPhysicalDataType().toLowerCase();
            switch (typeStrLowerCase) {
                case "bit":
                case "boolean":
                    type = Schema.Type.BOOLEAN;
                    break;
                case "byte":
                case "short":
                case "int":
                    type = Schema.Type.INT;
                    break;
                case "long":
                case "date":
                case "datetime":
                case "datetimeoffset":
                    type = Schema.Type.LONG;
                    break;
                case "float":
                    type = Schema.Type.FLOAT;
                    break;
                case "double":
                    type = Schema.Type.DOUBLE;
                    break;
                case "string":
                    type = Schema.Type.STRING;
                    break;
                default:
                    break;
            }
            if (type == null) {
                if (typeStrLowerCase.startsWith("nvarchar") || typeStrLowerCase.startsWith("varchar")) {
                    type = Schema.Type.STRING;
                }
            }
            return type;
        } else {
            return null;
        }
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

    void checkTemplateTable(Table finalTemplate, String entity, boolean withoutId, boolean enableEntityMatch, boolean onlyGA,
                            List<FieldValidation> validations,
                            Map<ValidationCategory, List<FieldValidation>> groupedValidations) {
        Map<String, Attribute> standardAttrs = new HashMap<>();
        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true,
                withoutId, enableEntityMatch, onlyGA);
        standardTable.getAttributes().forEach(attribute -> standardAttrs.put(attribute.getName(), attribute));
        Map<String, Attribute> templateAttrs = new HashMap<>();
        finalTemplate.getAttributes().forEach(attribute -> templateAttrs.put(attribute.getName(), attribute));
        for (Map.Entry<String, Attribute> attrEntry : standardAttrs.entrySet()) {
            if (attrEntry.getValue().getRequired() && attrEntry.getValue().getDefaultValueStr() == null) {
                if(!templateAttrs.containsKey(attrEntry.getKey())) {
                    String message = String
                            .format("%s is not mapped, and is a required field.", attrEntry.getKey());
                    FieldValidation validation = createValidation(null, attrEntry.getKey(), ValidationStatus.ERROR,
                            message);
                    validations.add(validation);
                    groupedValidations.get(ValidationCategory.RequiredField).add(validation);
                }
            }
            if (templateAttrs.containsKey(attrEntry.getKey())) {
                Attribute attr1 = attrEntry.getValue();
                Attribute attr2 = templateAttrs.get(attrEntry.getKey());
                String nameFromFile = attr2.getSourceAttrName() == null ? attr2.getDisplayName() :
                        attr2.getSourceAttrName();
                if (!attr1.getPhysicalDataType().equalsIgnoreCase(attr2.getPhysicalDataType())) {
                    String message = "Data type is not the same for attribute: " + attr1.getDisplayName();
                    FieldValidation validation = createValidation(nameFromFile, attr2.getName(),
                            ValidationStatus.ERROR, message);
                    validations.add(validation);
                    groupedValidations.get(ValidationCategory.DataType).add(validation);
                }
                if (!attr1.getRequired().equals(attr2.getRequired())) {
                    String message = "Required flag is not the same for attribute: " + attr1.getDisplayName();
                    FieldValidation validation = createValidation(nameFromFile, attr2.getName(),
                            ValidationStatus.ERROR, message);
                    validations.add(validation);
                    groupedValidations.get(ValidationCategory.RequiredField).add(validation);
                }
            }
        }
    }

    @Override
    public void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument, boolean isModel,
                                boolean enableEntityMatch, boolean onlyGA) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        SchemaInterpretation schemaInterpretation = sourceFile.getSchemaInterpretation();
        schemaInterpretation = enableEntityMatch && isModel && schemaInterpretation.equals(SchemaInterpretation.Account) ? SchemaInterpretation.ModelAccount : schemaInterpretation;
        Table table = getTableFromParameters(schemaInterpretation, false, enableEntityMatch, onlyGA);
        resolveMetadata(sourceFile, fieldMappingDocument, table, false, null, null);
    }

    private Table generateTemplate(String sourceFileName, FieldMappingDocument fieldMappingDocument, String entity,
                                   String source, String feedType) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        Table table, schemaTable;
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEntityMatchEnabled(customerSpace);
        if (dataFeedTask == null) {
            table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                    enableEntityMatch, batonService.onlyEntityMatchGAEnabled(customerSpace));
        } else {
            table = dataFeedTask.getImportTemplate();
        }
        schemaTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                enableEntityMatch, batonService.onlyEntityMatchGAEnabled(customerSpace));
        // this is to avoid the exception in following steps, e.g. resolve metadata
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            if (fieldMapping.getCdlExternalSystemType() != null) {
                fieldMapping.setMappedToLatticeField(false);
            }
        }
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
        SourceFile sourceFile = getSourceFile(sourceFileName);
        Table table, schemaTable;
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);
        BusinessEntity businessEntity = BusinessEntity.getByName(entity);
        schemaTable = getSchemaTable(customerSpace, businessEntity, feedType, withoutId);
        S3ImportSystem defaultSystem = cdlService.getDefaultImportSystem(customerSpace.toString());
        /*
         * store fieldmappings with idtype matched. Regulatefiledmapping function may
         * delete idtype with length and required info returned by UI
         */
        List<FieldMapping> fieldMappingsWithIdtype = fieldMappingDocument.getFieldMappings().stream()
                .filter(f -> idTypeMatchEntity(f, businessEntity)).collect(Collectors.toList());
        if (dataFeedTask == null) {
            table = TableUtils.clone(schemaTable, schemaTable.getName());
            EntityMatchGAConverterUtils.setSystemIdForGATenant(enableEntityMatch, enableEntityMatchGA,
                    fieldMappingDocument, defaultSystem);
            regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity), feedType, null);
            EntityMatchGAConverterUtils.convertSavingMappings(enableEntityMatch, enableEntityMatchGA,
                    fieldMappingDocument, defaultSystem);
        } else {
            table = dataFeedTask.getImportTemplate();
            EntityMatchGAConverterUtils.setSystemIdForGATenant(enableEntityMatch, enableEntityMatchGA,
                    fieldMappingDocument, defaultSystem);
            regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity), feedType, table);
            if (table.getAttribute(InterfaceName.AccountId) == null) {
                EntityMatchGAConverterUtils.convertSavingMappings(enableEntityMatch, enableEntityMatchGA,
                        fieldMappingDocument, defaultSystem);
            }
        }
        resolveMetadata(sourceFile, fieldMappingDocument, table, true, schemaTable, BusinessEntity.getByName(entity));
        if (BusinessEntity.Account.equals(businessEntity) || BusinessEntity.Contact.equals(businessEntity)) {
            updateSourceFileConfig(customerSpace, sourceFile, fieldMappingsWithIdtype, businessEntity, feedType);
        }
    }

    /*
     * update length validator : UI might return duplicate idtypes, if there's more
     * than 1 with different fieldname,use isMappedToLatticeSystem to filter the old
     * idtype
     */
    private void updateSourceFileConfig(CustomerSpace customerSpace, SourceFile sourceFile,
                                        List<FieldMapping> fieldMappingsWithIdtype, BusinessEntity entity, String feedType) {
        String systemName = cdlService.getSystemNameFromFeedType(feedType);
        if (StringUtils.isBlank(systemName)) {
            return;
        }
        S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
        String systemId;
        if (s3ImportSystem == null || (systemId = getSystemId(s3ImportSystem, entity)) == null) {
            return;
        }
        fieldMappingsWithIdtype = fieldMappingsWithIdtype.stream()
                .filter(f -> (systemName.equals(f.getSystemName()) && systemId.equals(f.getMappedField())))
                .collect(Collectors.toList());
        //if multiple user field mapped to systemId, use the new one(not mappedtolatticesystem)
        //if same user field mapped to systemId, iterate to find config
        boolean mappedCondition = fieldMappingsWithIdtype.stream().map(FieldMapping::getUserField)
                .collect(Collectors.toSet()).size() > 1 ? true : false;
        for (FieldMapping fieldMapping : fieldMappingsWithIdtype) {
            if (mappedCondition && fieldMapping.isMappedToLatticeSystem()) {
                continue;
            }
            if (fieldMapping.lengthAndRequiredChanged() && fieldMapping.checkLengthValid()) {
                UniqueIdentifierConfig uniqueConfig = new UniqueIdentifierConfig();
                uniqueConfig.setName(fieldMapping.getMappedField());
                uniqueConfig.setLength(fieldMapping.getLength());
                uniqueConfig.setRequired(fieldMapping.isRequired());
                SourcefileConfig sourceConfig = sourceFile.getSourcefileConfig();
                if (sourceConfig == null) {
                    sourceConfig = new SourcefileConfig();
                }
                sourceConfig.setUniqueIdentifierConfig(uniqueConfig);
                sourceFile.setSourcefileConfig(sourceConfig);
                sourceFileService.update(sourceFile);
                return;
            }
        }
    }

    private boolean idTypeMatchEntity(FieldMapping fieldMapping, BusinessEntity entity) {
        if (fieldMapping == null || fieldMapping.getIdType() == null)
            return false;
        switch (fieldMapping.getIdType()) {
            case Account:
                return BusinessEntity.Account.equals(entity);
            case Contact:
                return BusinessEntity.Contact.equals(entity);
            default:
                return false;
        }
    }

    private String getSystemId(S3ImportSystem s3ImportSystem, BusinessEntity entity) {
        if (BusinessEntity.Account.equals(entity)) {
            return s3ImportSystem.getAccountSystemId();
        } else if (BusinessEntity.Contact.equals(entity)) {
            return s3ImportSystem.getContactSystemId();
        } else {
            return null;
        }
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, CloseableResourcePool leCsvParser, String fileName,
                                            boolean checkHeaderFormat) {
        return validateHeaderFields(stream, leCsvParser, fileName, checkHeaderFormat, null);
    }

    private Table getSchemaTable(CustomerSpace customerSpace, BusinessEntity entity, String feedType, boolean withoutId) {
        String systemName = EntityTypeUtils.getSystemName(feedType);
        EntityType entityType = EntityTypeUtils.matchFeedType(feedType);
        Table schemaTable;
        boolean enableEntityMatch = batonService.isEntityMatchEnabled(customerSpace);
        if (StringUtils.isNotEmpty(systemName) && entityType != null) {
            S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
            schemaTable = SchemaRepository.instance().getSchema(s3ImportSystem.getSystemType(), entityType,
                    enableEntityMatch, batonService.onlyEntityMatchGAEnabled(customerSpace));
        } else {
            schemaTable = SchemaRepository.instance().getSchema(entity, true, withoutId, enableEntityMatch,
                    batonService.onlyEntityMatchGAEnabled(customerSpace));
        }
        return schemaTable;
    }

    // Assumption: UI might send duplicate system id mapping, remove the one that already map to systemId (old one)
    private void removeDuplicateSystemIdMapping(FieldMappingDocument fieldMappingDocument, Set<String> systemIdSet) {
        if (CollectionUtils.isEmpty(systemIdSet)) {
            return;
        }
        Map<String, FieldMapping> systemIdMapping = new HashMap<>();
        List<FieldMapping> mappingToRemove = new ArrayList<>();
        fieldMappingDocument.getFieldMappings().forEach(fieldMapping -> {
            if (fieldMapping.getIdType() != null && StringUtils.isNotEmpty(fieldMapping.getSystemName())
                    && !fieldMapping.isMappedToLatticeSystem()) {
                String key = fieldMapping.getIdType() + "|" + fieldMapping.getSystemName();
                if (systemIdMapping.containsKey(key)) {
                    if (StringUtils.isNotEmpty(fieldMapping.getMappedField()) && systemIdSet.contains(fieldMapping.getMappedField())) {
                        mappingToRemove.add(fieldMapping);
                    } else if (StringUtils.isNotEmpty(systemIdMapping.get(key).getMappedField())
                            && systemIdSet.contains(systemIdMapping.get(key).getMappedField())) {
                        mappingToRemove.add(systemIdMapping.get(key));
                    } else {
                        log.error("There's duplicate system id mapping but cannot determine which one to remove! {} vs {}",
                                JsonUtils.serialize(fieldMapping), JsonUtils.serialize(systemIdMapping.get(key)));
                    }
                } else {
                    systemIdMapping.put(key, fieldMapping);
                }
            }
        });
        if (CollectionUtils.isNotEmpty(mappingToRemove)) {
            fieldMappingDocument.getFieldMappings().removeAll(mappingToRemove);
        }
    }

    private void regulateFieldMapping(FieldMappingDocument fieldMappingDocument, BusinessEntity entity, String feedType,
                                      Table templateTable) {
        if (fieldMappingDocument == null || fieldMappingDocument.getFieldMappings() == null
                || fieldMappingDocument.getFieldMappings().size() == 0) {
            return;
        }
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        // 1. set system related mapping //only apply to Account / Contact / Transaction
        Set<String> systemIdSet = cdlService.getAllS3ImportSystemIdSet(customerSpace.toString());
        if (BusinessEntity.Account.equals(entity) || BusinessEntity.Contact.equals(entity) || BusinessEntity.Transaction.equals(entity)) {
            removeDuplicateSystemIdMapping(fieldMappingDocument, systemIdSet);
            List<FieldMapping> customerLatticeIdList = new ArrayList<>();
            Iterator<FieldMapping> iterator = fieldMappingDocument.getFieldMappings().iterator();
            while (iterator.hasNext()) {
                FieldMapping fieldMapping = iterator.next();
                if (fieldMapping.getIdType() != null && !fieldMapping.isMappedToLatticeSystem()) {
                    setSystemIdMapping(customerSpace, feedType, customerLatticeIdList, fieldMapping);
                } else {
                    // Assumption: user cannot map column to SystemId, so remove error fieldMapping provided by UI.
                    if (StringUtils.isNotEmpty(fieldMapping.getMappedField()) && systemIdSet.contains(fieldMapping.getMappedField())) {
                        iterator.remove();
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
        Table schemaTable = getSchemaTable(customerSpace, entity, feedType, withoutId);
        Table standardTable = templateTable == null ? schemaTable : templateTable;
        Set<String> reservedName = standardTable.getAttributes().stream().map(Attribute::getName)
                .collect(Collectors.toSet());
        Set<String> mappedFieldName = new HashSet<>();
        Set<String> pendingUserFieldName = new HashSet<>();
        Set<String> mappedUserFieldName = new HashSet<>();
        List<String> ignoredFields = ListUtils.emptyIfNull(fieldMappingDocument.getIgnoredFields());
        // 2.check if there's multiple mapping to standard field
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (ignoredFields.contains(fieldMapping.getUserField())) {
                continue;
            }
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

    private void setSystemIdMapping(CustomerSpace customerSpace, String feedType,
                                    List<FieldMapping> customerLatticeIdList, FieldMapping fieldMapping) {
        String systemName = cdlService.getSystemNameFromFeedType(feedType);
        EntityType entityType = EntityTypeUtils.matchFeedType(feedType);
        if (StringUtils.isEmpty(systemName)) {
            return;
        }
        if (StringUtils.isEmpty(fieldMapping.getSystemName())) { // Won't accept empty system name now.
            log.warn("Should not use empty System name to identify itself, will deprecated soon!");
            //Todo: throw exception for empty system name case
            //throw new IllegalArgumentException("Target system name cannot be empty!");

            // For now: auto set the system name and IdType (assume this is for id of it's own.)
            fieldMapping.setSystemName(systemName);
            if (FieldMapping.IdType.Contact.equals(fieldMapping.getIdType()) && EntityType.Leads.equals(entityType)) {
                fieldMapping.setIdType(FieldMapping.IdType.Lead);
            }
            if (idTypeMatchEntityType(fieldMapping, entityType)) {
                setSelfSystemId(customerSpace, customerLatticeIdList, fieldMapping, systemName, entityType);
            } else {
                setMappingSystemId(customerSpace, customerLatticeIdList, fieldMapping);
            }
        } else if (systemName.equals(fieldMapping.getSystemName())) {
            // Map System Id for it's own.
            // We will use straightforward info to identify primary & secondary system instead of combination of inputs.
            // Primary system: IdType Account / Contact
            // Secondary system: IdType Lead
            if (idTypeMatchEntityType(fieldMapping, entityType)) {
                setSelfSystemId(customerSpace, customerLatticeIdList, fieldMapping, systemName, entityType);
            } else {
                setMappingSystemId(customerSpace, customerLatticeIdList, fieldMapping);
            }
        } else { // Map current field to another system.
            setMappingSystemId(customerSpace, customerLatticeIdList, fieldMapping);
        }
    }

    private boolean idTypeMatchEntityType(FieldMapping fieldMapping, EntityType currentEntity) {
        Preconditions.checkNotNull(fieldMapping);
        Preconditions.checkNotNull(fieldMapping.getIdType());
        boolean match = false;
        switch (fieldMapping.getIdType()) {
            case Account:
                match = EntityType.Accounts.equals(currentEntity);
                break;
            case Contact:
                match = EntityType.Contacts.equals(currentEntity);
                break;
            case Lead:
                match = EntityType.Leads.equals(currentEntity);
                break;
            default:
                break;
        }
        return match;
    }

    private void setMappingSystemId(CustomerSpace customerSpace, List<FieldMapping> customerLatticeIdList, FieldMapping fieldMapping) {
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
            case Lead:
                throw new IllegalArgumentException("Cannot map Id to Lead template.");
            default:
                throw new IllegalArgumentException("Unrecognized idType: " + fieldMapping.getIdType());
        }
    }

    private void setSelfSystemId(CustomerSpace customerSpace, List<FieldMapping> customerLatticeIdList,
                                 FieldMapping fieldMapping, String systemName, EntityType entityType) {
        fieldMapping.setFieldType(UserDefinedType.TEXT);
        S3ImportSystem importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
        switch (fieldMapping.getIdType()) {
            case Account:
                String accountSystemId = importSystem.getAccountSystemId();
                importSystem.setMapToLatticeAccount(importSystem.isMapToLatticeAccount() || fieldMapping.isMapToLatticeId());
                cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                if (StringUtils.isEmpty(accountSystemId)) {
                    accountSystemId = importSystem.generateAccountSystemId();
                    importSystem.setAccountSystemId(accountSystemId);
                    cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                    fieldMapping.setMappedToLatticeField(false);
                } else {
                    fieldMapping.setMappedToLatticeField(accountSystemId.equals(fieldMapping.getMappedField()));
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
                importSystem.setMapToLatticeContact(importSystem.isMapToLatticeContact() || fieldMapping.isMapToLatticeId());
                cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                if (StringUtils.isEmpty(contactSystemId)) {
                    contactSystemId = importSystem.generateContactSystemId();
                    importSystem.setContactSystemId(contactSystemId);
                    cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                    fieldMapping.setMappedToLatticeField(false);
                } else {
                    fieldMapping.setMappedToLatticeField(contactSystemId.equals(fieldMapping.getMappedField()));
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
            case Lead:
                if (EntityType.Leads.equals(importSystem.getSystemType().getPrimaryContact())) {
                    // This is primary System
                    String leadSystemId = importSystem.getContactSystemId();
                    importSystem.setMapToLatticeContact(importSystem.isMapToLatticeContact() || fieldMapping.isMapToLatticeId());
                    cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                    importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                    if (StringUtils.isEmpty(leadSystemId)) {
                        leadSystemId = importSystem.generateContactSystemId();
                        importSystem.setContactSystemId(leadSystemId);
                        cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                        fieldMapping.setMappedToLatticeField(false);
                    } else {
                        fieldMapping.setMappedToLatticeField(leadSystemId.equals(fieldMapping.getMappedField()));
                    }
                    fieldMapping.setMappedField(leadSystemId);
                    if (importSystem.isMapToLatticeContact()) {
                        FieldMapping customerLatticeId = new FieldMapping();
                        customerLatticeId.setUserField(fieldMapping.getUserField());
                        customerLatticeId.setMappedField(InterfaceName.CustomerContactId.name());
                        customerLatticeId.setFieldType(fieldMapping.getFieldType());
                        customerLatticeIdList.add(customerLatticeId);
                    }

                } else {
                    // secondary System
                    String leadSystemId = importSystem.getSecondaryContactId(entityType);
                    if (StringUtils.isEmpty(leadSystemId)) {
                        leadSystemId = importSystem.generateContactSystemId();
                        importSystem.addSecondaryContactId(entityType, leadSystemId);
                        cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                        fieldMapping.setMappedToLatticeField(false);
                    } else {
                        fieldMapping.setMappedToLatticeField(leadSystemId.equals(fieldMapping.getMappedField()));
                    }
                    fieldMapping.setMappedField(leadSystemId);
                }
                break;
            default:
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

        Preconditions.checkNotNull(MultiTenantContext.getCustomerSpace());
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
            CDLExternalSystemUtils.setCDLExternalSystem(resolver.getExternalSystem(), entity, cdlExternalSystemProxy);
        }
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, CloseableResourcePool closeableResourcePool,
                                            String fileDisplayName, boolean checkHeaderFormat, String entity) {
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
        ValidateFileHeaderUtils.checkForDuplicatedHeaders(headerFields);
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
        Preconditions.checkNotNull(customerSpace);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        if (dataFeedTask == null) {
            boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
            boolean enableEntityMatch = batonService.isEntityMatchEnabled(customerSpace);
            Table table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                    enableEntityMatch, batonService.onlyEntityMatchGAEnabled(customerSpace));
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

    private Map<ValidationCategory, List<FieldValidation>> initializeGroupedValidation() {
        Map<ValidationCategory, List<FieldValidation>> groupedValidation = new LinkedHashMap();
        for (ValidationCategory val : ValidationCategory.values()) {
            groupedValidation.put(val, new ArrayList<>());
        }
        return groupedValidation;
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

