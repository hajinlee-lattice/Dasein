package com.latticeengines.pls.service.impl;

import static com.latticeengines.pls.util.ImportWorkflowUtils.validateFieldDefinitionRequestParameters;
import static com.latticeengines.pls.util.ImportWorkflowUtils.validateFieldDefinitionsRequestBody;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.app.exposed.service.impl.CommonTenantConfigServiceImpl;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
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
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.InputValidatorWrapper;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SchemaInterpretationFunctionalInterface;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.ExtraFieldMappingInfo;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation.ValidationStatus;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationResult;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.OtherTemplateData;
import com.latticeengines.domain.exposed.pls.frontend.RequiredType;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.ImportWorkflowSpecUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.validation.ReservedField;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.EntityMatchGAConverterUtils;
import com.latticeengines.pls.util.ImportWorkflowUtils;
import com.latticeengines.pls.util.SystemIdsUtils;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

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
    private TenantService tenantService;

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Inject
    private CommonTenantConfigServiceImpl appTenantConfigService;

    @Autowired
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Autowired
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Override
    public FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName,
                                                                  SchemaInterpretation schemaInterpretation, ModelingParameters parameters, boolean isModel, boolean withoutId,
                                                                  boolean enableEntityMatch) {
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
        String systemName = EntityTypeUtils.getSystemName(feedType);
        EntityType entityType = EntityTypeUtils.matchFeedType(feedType);
        SchemaInterpretation schemaInterpretation;
        Table table;
        if (StringUtils.isNotEmpty(systemName) && entityType != null) {
            S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
            schemaInterpretation = entityType.getSchemaInterpretation();
            table = SchemaRepository.instance().getSchema(s3ImportSystem.getSystemType(), entityType,
                    batonService.isEntityMatchEnabled(customerSpace));

        } else {
            schemaInterpretation = SchemaInterpretation.getByName(entity);
            boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
            table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                    batonService.isEntityMatchEnabled(customerSpace));
        }
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        FieldMappingDocument fieldMappingFromSchemaRepo = resolver.getFieldMappingsDocumentBestEffort(table);
        generateExtraFieldMappingInfo(fieldMappingFromSchemaRepo, true);
        FieldMappingDocument resultDocument;
        if (dataFeedTask == null) {
            resultDocument = fieldMappingFromSchemaRepo;
        } else {
            Table templateTable = dataFeedTask.getImportTemplate();
            FieldMappingDocument fieldMappingFromTemplate = getFieldMappingBaseOnTable(sourceFile, templateTable);
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
                setSystemMapping(customerSpace.toString(), systemName, fieldMappingFromTemplate);
            }
            resultDocument = mergeFieldMappingBestEffort(fieldMappingFromTemplate, fieldMappingFromSchemaRepo,
                    templateTable, table);
        }
        EntityMatchGAConverterUtils.convertGuessingMappings(enableEntityMatch, enableEntityMatchGA, resultDocument);
        return resultDocument;
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
        generateExtraFieldMappingInfo(templateMapping, false);
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
    public FieldValidationResult validateFieldMappings(String sourceFileName, FieldMappingDocument fieldMappingDocument,
                                                       String entity, String source, String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);

        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);
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
        // multiple lattice field mapped to the same user field, the value of map should be a list
        Map<String, List<FieldMapping>> userFieldMap = fieldMappings.stream()
                .filter(mapping -> StringUtils.isNotBlank(mapping.getUserField()))
                .collect(Collectors.groupingBy(FieldMapping::getUserField));
        Set<String> standardAttrNames =
                standardTable.getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet());

        Set<String> mappedStandardFields = new HashSet<>();
        // check if there's multiple mapping to standard field
        for (FieldMapping fieldMapping : fieldMappings) {
            if (fieldMapping.getMappedField() != null) {
                if (standardAttrNames.contains(fieldMapping.getMappedField())) {
                    if (mappedStandardFields.contains(fieldMapping.getMappedField())) {
                        String message =
                                "Multiple user fields are mapped to standard field " + fieldMapping.getMappedField();
                        validations.add(createValidation(fieldMapping.getUserField(), fieldMapping.getMappedField(),
                                ValidationStatus.ERROR, message));
                    } else {
                        mappedStandardFields.add(fieldMapping.getMappedField());
                    }
                }
            }
        }

        // compare field mapping document after being modified with field mapping best effort
        for (FieldMapping bestEffortMapping : documentBestEffort.getFieldMappings()) {
            String userField = bestEffortMapping.getUserField();
            // skip user field mapped to standard attribute or user ignored fields
            if (StringUtils.isNotBlank(userField) && !ignored.contains(userField)) {
                List<FieldMapping> fieldMappingGroup = userFieldMap.get(userField);
                if (CollectionUtils.isEmpty(fieldMappingGroup)) {
                    continue;
                }
                for (FieldMapping fieldMapping : fieldMappingGroup) {
                    if (!standardAttrNames.contains(fieldMapping.getMappedField()) && bestEffortMapping.getFieldType() != fieldMapping.getFieldType()) {
                        String message = String
                                .format("%s is set as %s but appears to only have %s values.", userField, fieldMapping.getFieldType(),
                                        bestEffortMapping.getFieldType());
                        validations.add(createValidation(userField, fieldMapping.getMappedField(), ValidationStatus.WARNING,
                                message));
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
                            validations.add(createValidation(userField, fieldMapping.getMappedField(),
                                    ValidationStatus.WARNING, warningMessage.toString()));
                        } else if (StringUtils.isNotBlank(userFormat) && !userFormat.equals(formatWithBestEffort)) {
                            // this is case that user change the date/time format which can be parsed
                            String message =  String.format("%s is set as %s which can parse the value from uploaded " +
                                    "file.", userField, userFormat);
                            validations.add(createValidation(userField, fieldMapping.getMappedField(),
                                    ValidationStatus.WARNING, message));
                        }
                    }
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
        FieldValidationResult fieldValidationResult = new FieldValidationResult();
        int limit;
        if (BusinessEntity.Account.name().equals(entity)) {
            limit = appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(MultiTenantContext.getShortTenantId(), DataLicense.ACCOUNT.getDataLicense());
            validateFieldSize(fieldValidationResult, customerSpace, entity, generatedTemplate, limit, enableEntityMatch, enableEntityMatchGA);
        } else if (BusinessEntity.Contact.name().equals(entity)) {
            limit = appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(MultiTenantContext.getShortTenantId(), DataLicense.CONTACT.getDataLicense());
            validateFieldSize(fieldValidationResult, customerSpace, entity, generatedTemplate, limit, enableEntityMatch, enableEntityMatchGA);
        }
        Table finalTemplate = mergeTable(templateTable, generatedTemplate);
        // compare type, require flag between template and standard schema
        checkTemplateTable(finalTemplate, entity, withoutId, enableEntityMatch, validations);
        fieldValidationResult.setFieldValidations(validations);
        return fieldValidationResult;
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
        Boolean convertName = enableEntityMatchGA && !enableEntityMatch;
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
        ValidateFileHeaderUtils.exceedQuotaFieldSize(fieldValidationResult, fieldSize, limit);
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
        } else {
            table = dataFeedTask.getImportTemplate();
        }
        schemaTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId, enableEntityMatch);
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
        decodeFieldMapping(fieldMappingDocument);
        SourceFile sourceFile = getSourceFile(sourceFileName);
        Table table, schemaTable;
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);
        BusinessEntity businessEntity = BusinessEntity.getByName(entity);
        schemaTable = getSchemaTable(customerSpace, businessEntity, feedType, withoutId);
        if (dataFeedTask == null) {
            table = TableUtils.clone(schemaTable, schemaTable.getName());
            regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity), feedType, null);
            EntityMatchGAConverterUtils.convertSavingMappings(enableEntityMatch, enableEntityMatchGA, fieldMappingDocument);
        } else {
            table = dataFeedTask.getImportTemplate();
            regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity), feedType, table);
            if (table.getAttribute(InterfaceName.AccountId) == null) {
                EntityMatchGAConverterUtils.convertSavingMappings(enableEntityMatch, enableEntityMatchGA, fieldMappingDocument);
            }
        }
        resolveMetadata(sourceFile, fieldMappingDocument, table, true, schemaTable, BusinessEntity.getByName(entity));
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, CloseableResourcePool leCsvParser, String fileName,
                                            boolean checkHeaderFormat) {
        return validateHeaderFields(stream, leCsvParser, fileName, checkHeaderFormat, null);
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

    private Table getSchemaTable(CustomerSpace customerSpace, BusinessEntity entity, String feedType, boolean withoutId) {
        String systemName = EntityTypeUtils.getSystemName(feedType);
        EntityType entityType = EntityTypeUtils.matchFeedType(feedType);
        Table schemaTable;
        boolean enableEntityMatch = batonService.isEntityMatchEnabled(customerSpace);
        if (StringUtils.isNotEmpty(systemName) && entityType != null) {
            S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
            schemaTable = SchemaRepository.instance().getSchema(s3ImportSystem.getSystemType(), entityType, enableEntityMatch);
        } else {
            schemaTable = SchemaRepository.instance().getSchema(entity, true, withoutId, enableEntityMatch);
        }
        return schemaTable;
    }

    private void regulateFieldMapping(FieldMappingDocument fieldMappingDocument, BusinessEntity entity, String feedType,
                                      Table templateTable) {
        if (fieldMappingDocument == null || fieldMappingDocument.getFieldMappings() == null
                || fieldMappingDocument.getFieldMappings().size() == 0) {
            return;
        }
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        // 1. set system related mapping //only apply to Account / Contact
        if (BusinessEntity.Account.equals(entity) || BusinessEntity.Contact.equals(entity)) {
            List<FieldMapping> customerLatticeIdList = new ArrayList<>();
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (fieldMapping.getIdType() != null && !fieldMapping.isMappedToLatticeSystem()) {
                    setSystemIdMapping(customerSpace, feedType, customerLatticeIdList, fieldMapping);
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
            setSelfSystemId(customerSpace, customerLatticeIdList, fieldMapping, systemName, entityType);
        } else if (systemName.equals(fieldMapping.getSystemName())) {
            // Map System Id for it's own.
            // We will use straightforward info to identify primary & secondary system instead of combination of inputs.
            // Primary system: IdType Account / Contact
            // Secondary system: IdType Lead
            setSelfSystemId(customerSpace, customerLatticeIdList, fieldMapping, systemName, entityType);
        } else { // Map current field to another system.
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
                break;
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

    @Override
    public FetchFieldDefinitionsResponse fetchFieldDefinitions(String systemName, String systemType,
                                                               String systemObject, String importFile)
            throws Exception {

        log.info("JAW ------ BEGIN Real Fetch Field Definition -----");

        // 1. Validate HTTP request parameters.
        validateFieldDefinitionRequestParameters("Fetch", systemName, systemType, systemObject, importFile);

        // 2. Generate extra fields from older parameters to interact with proxies and services.
        // systemObject ==> entityType
        // systemName + entityType ==> feedType
        // "File" ==> source   [hard coded for now, since options are File or VisiDB, which doesn't need to be
        //                      supported.  Confirm this!]
        // importFile ==> sourceFile
        // entityType + sourceFile ==> schemaInterpretation
        // ==> customerSpace

        // 2a. Convert systemObject to entity.
        EntityType entityType = EntityType.fromDisplayNameToEntityType(systemObject);

        // 2b. Convert systemName and entityType to feedType.
        String feedType = EntityTypeUtils.generateFullFeedType(systemName, entityType);

        // 2c. Generate source string.
        // TODO(jwinter): Assume source is always "File".  I don't think VisiDB support is needed.
        String source = "File";

        // 2d. Generate sourceFile object.
        SourceFile sourceFile = getSourceFile(importFile);

        // 2e. Update schema interpretation.
        // TODO(jwinter): Figure out what this is for.
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entityType.getEntity().name());
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }

        // 2f. Generate customerSpace.
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();

        log.info(String.format("Internal Values:\n   entity: %s\n   subType: %s\n   feedType: %s\n   source: %s\n" +
                        "   Source File: %s\n   Customer Space: %s", entityType.getEntity(), entityType.getSubType(),
                feedType, source, sourceFile.getName(), customerSpace.toString()));

        // 3. Get flags relevant to import workflow.
        // TODO(jwinter): Figure out how to incorporate all the system flags for entity match.
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);

        // TODO(jwinter): Need to incorporate Batch Store into initial generation of FetchFieldDefinitionsResponse.
        // 4. Generate FetchFieldMappingResponse by combining:
        //    a. A FieldDefinitionRecord to hold the current field definitions.
        //    b. Spec for this system.
        //    c. Existing field definitions from DataFeedTask.
        //    d. Columns and autodetection results from the sourceFile.
        //    e. Other System Templates matching this System Object (TODO).
        //    f. Batch Store (TODO)

        // 4a. Setup up FetchFieldDefinitionsResponse and Current FieldDefinitionsRecord.
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse = new FetchFieldDefinitionsResponse();
        fetchFieldDefinitionsResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord(systemName, systemType, systemObject));

        // 4b. Retrieve Spec for given systemType and systemObject.
        fetchFieldDefinitionsResponse.setImportWorkflowSpec(
                importWorkflowSpecProxy.getImportWorkflowSpec(customerSpace.toString(), systemType, systemObject));

        // 4c. Find previously saved template matching this customerSpace, source, feedType, and entityType, if it
        // exists.
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType,
                entityType.getEntity().name());
        printDataFeedTask("Existing DataFeedTask Template", dataFeedTask);
        Table existingTable = null;
        Map<String, FieldDefinition> existingFieldDefinitionsMap = null;
        if (dataFeedTask != null) {
            existingTable = dataFeedTask.getImportTemplate();
            fetchFieldDefinitionsResponse.setExistingFieldDefinitionsMap(
                    ImportWorkflowUtils.getFieldDefinitionsMapFromTable(existingTable));
        }

        // 4d. Create a MetadataResolver using the sourceFile.
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        fetchFieldDefinitionsResponse.setAutodetectionResultsMap(
                ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));

        // 4e. Fetch all other DataFeedTask templates for Other Systems that are the same System Object and extract
        // the fieldTypes used for each field.
        // TODO(jwinter): Add Other System processing.

        // 4f. Get the Metadata Attribute data from the Batch Store and get the fieldTypes set there.
        // TODO(jwinter):  Implement Batch Store extractions.

        // 5. Generate the initial FieldMappingsRecord based on the Spec, existing table, input file, and batch store.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(fetchFieldDefinitionsResponse);

        log.info("JAW ------ END Real Fetch Field Definition -----");

        return fetchFieldDefinitionsResponse;
    }

    // TODO(jwinter): Steps being left for validation:
    //   1. Make sure the new metadata table has all the required attributes from the Spec.
    //   2. Get the set of existing templates for all system types that match this entity.
    //      a. If the existing templates have lower cased attributes names, lower case the new table’s attribute
    //         names.
    //      b. Make sure the physical types of attributes in the new table match those of the existing templates.
    //   3. Check that the new metadata table hasn't changed physicalDataTypes of attributes compared to the
    //      existing template, if DataFeed state is not DataFeed.Status.Initing.
    //   4. Final checks against Spec:
    //      a. All required attributes are there.
    //      b. Physical types may differ for special cases.  (this may no longer be applicable)
    //      c. Verify Spec field are not changed.



    @Override
    public FieldDefinitionsRecord commitFieldDefinitions(String systemName, String systemType, String systemObject,
                                                         String importFile, boolean runImport,
                                                         FieldDefinitionsRecord commitRequest)
            throws LedpException, IllegalArgumentException {

        log.info("JAW ------ BEGIN Real Commit Field Definition -----");

        // 1a. Validate HTTP request parameters.
        validateFieldDefinitionRequestParameters("Commit", systemName, systemType, systemObject, importFile);

        // 1b. Validate Commit Request.
        validateFieldDefinitionsRequestBody("Commit", commitRequest);

        // 2. Generate extra fields from older parameters to interact with proxies and services.
        // systemObject ==> entityType
        // systemName + entityType ==> feedType
        // "File" ==> source   [hard coded for now, since options are File or VisiDB, which doesn't need to be
        //                      supported.  Confirm this!]
        // importFile ==> sourceFile
        // entityType + sourceFile ==> schemaInterpretation
        // ==> customerSpace

        // 2a. Convert systemObject to entity.
        EntityType entityType = EntityType.fromDisplayNameToEntityType(systemObject);

        // 2b. Convert systemName and entityType to feedType.
        String feedType = EntityTypeUtils.generateFullFeedType(systemName, entityType);

        // 2c. Generate source string.
        // TODO(jwinter): Assume source is always "File".  I don't think VisiDB support is needed.
        String source = "File";

        // 2d. Generate sourceFile object.
        SourceFile sourceFile = getSourceFile(importFile);

        // 2e. Update schema interpretation.
        // TODO(jwinter): Figure out what this is for.
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entityType.getEntity().name());
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }

        // 2f. Generate customerSpace.
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();

        log.info(String.format("Internal Values:\n   entity: %s\n   subType: %s\n   feedType: %s\n   source: %s\n" +
                        "   Source File: %s\n   Customer Space: %s", entityType.getEntity(), entityType.getSubType(),
                feedType, source, sourceFile.getName(), customerSpace.toString()));

        // 3. Process System IDs.  For now, this is only supported for entity types Accounts, Contacts, and Leads.
        if (EntityType.Accounts.equals(entityType) || EntityType.Contacts.equals(entityType) ||
                EntityType.Leads.equals(entityType)) {
            SystemIdsUtils.processSystemIds(customerSpace, systemName, systemType, entityType, commitRequest,
                    cdlService);
        }

        // 4. Generate new table from FieldDefinitionsRecord,
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        // TODO(jwinter): Figure out if the prefex should always be "SourceFile".
        String newTableName = "SourceFile_" + sourceFile.getName().replace(".", "_");
        Table newTable = ImportWorkflowSpecUtils.getTableFromFieldDefinitionsRecord(newTableName, false, commitRequest);
        // TODO(jwinter): Figure out how to properly set the Table display name.
        printTableAttributes("New Table", newTable);

        // 5. Delete old table associated with the source file from the database if it exists.
        if (sourceFile.getTableName() != null) {
            metadataProxy.deleteTable(customerSpace.toString(), sourceFile.getTableName());
        }

        // 6. Associate the new table with the source file and add new table to the database.
        metadataProxy.createTable(customerSpace.toString(), newTable.getName(), newTable);
        sourceFile.setTableName(newTable.getName());
        sourceFileService.update(sourceFile);

        // 7. Update the CDL External System data structures.
        // TODO(jwinter): Complete this work.
        // Set external system column name
        if (BusinessEntity.Account.equals(entityType.getEntity()) ||
                BusinessEntity.Contact.equals(entityType.getEntity())) {
            setCDLExternalSystem(resolver.getExternalSystem(), entityType.getEntity());
        }

        // 8. Create or Update the DataFeedTask
        String taskId = createOrUpdateDataFeedTask(newTable, customerSpace, source, feedType, entityType);

        // 9. Additional Steps
        // a. Update Attribute Configs.
        // b. Send email about S3 update.
        // TODO(jwinter): Do we need to add code to update the Attr Configs?
        // TODO(jwinter): Add code to send email about S3 Template change.


        // TODO(jwinter): Add flag to indicate a workflow job should be submitted and the code for submitting
        // workflow jobs.
        // 10. If requested, submit a workflow import job for this new template.
        if (runImport) {
            log.info("Running import workflow job for CustomerSpace {} and task ID {} on file {}",
                    customerSpace.toString(), taskId, importFile);
            cdlService.submitS3ImportWithTemplateData(customerSpace.toString(), taskId, importFile);
        }

        // 11. Setup the Commit Response for this request.
        // TODO(jwinter): Figure out what is the best commitResponse to provide.
        // Should the FieldDefinitionsRecord reflect any changes when new table is merged with
        // existing table?
        FieldDefinitionsRecord commitResponse = new FieldDefinitionsRecord();
        commitResponse.setFieldDefinitionsRecordsMap(commitRequest.getFieldDefinitionsRecordsMap());
        log.info("JAW ------ END Real Commit Field Definition -----");

        return commitResponse;
    }

    // Create or Update DataFeedTask with new table.
    // a. If there is an existing DataFeedTask template table.
    //   i. If the tables are identical with respect to data updated during import workflow template setup (name,
    //      displayName, physicalDataType, data/time formats, and timezone), there is no more steps to do.
    //   ii. Merge the new table and current DataFeedTask template table, adding new attributes and updating
    //       display name and data formats of old attributes.
    //   iii. Update the DataFeedTask with the new table.
    // b. If there is no existing DataFeedTask template table.
    //   i. Create a new DataFeedTask containing the new table.
    //   ii. Update the DataFeed status.

    // TODO(jwinter): Can we skip checking for an existing table, checking if the tables are identical, and
    // copying the changes into the existing table and just write the new table back?  The question is whether there
    // are fields and properties bucket values that are not related to import that have been stored in the
    // existing table's attributes?
    private String createOrUpdateDataFeedTask(Table newTable, CustomerSpace customerSpace, String source,
                                              String feedType, EntityType entityType) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType,
                entityType.getEntity().name());
        if (dataFeedTask != null) {
            log.info("Found existing DataFeedTask template: {}", dataFeedTask.getTemplateDisplayName());
            printTableAttributes("DataFeedTask", dataFeedTask.getImportTemplate());
            Table existingTable = dataFeedTask.getImportTemplate();
            if (!ImportWorkflowUtils.compareMetadataTables(existingTable, newTable)) {
                Table mergedTable = ImportWorkflowUtils.mergeMetadataTables(existingTable, newTable);
                printTableAttributes("Merged", mergedTable);
                dataFeedTask.setImportTemplate(mergedTable);
                dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            }
        } else {
            log.info("No existing DataFeedTask template");
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedTask.setImportTemplate(newTable);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entityType.getEntity().name());
            dataFeedTask.setFeedType(feedType);
            dataFeedTask.setSource(source);
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedTask.setSubType(entityType.getSubType());
            dataFeedTask.setTemplateDisplayName(entityType.getDefaultFeedTypeName());
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            // TODO(jwinter): Add DropBoxService stuff.

            // TODO(jwinter): Should we be doing this DataFeed status update?
            DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
            if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
                dataFeedProxy.updateDataFeedStatus(customerSpace.toString(), DataFeed.Status.Initialized.getName());
            }
        }
        return dataFeedTask.getUniqueId();
    }

    /**
     1. Required Spec (Lattice) Fields are mapped
     2. No fieldNames are mapped more than once
     3. If the standard Lattice fields are not mapped, they are included as “new” customer fields unless ignored by the user.
     4. Physical Data Type
     Compare physicalDataType against column data.
     5. Need to support special case allowed physical date type changes for backwards compatibility
     Deal with attribute name case sensitivity issues.
     6. DATE Type Validation
     Compare data and time format of DATE type against column data
     */
    @Override
    public ValidateFieldDefinitionsResponse validateFieldDefinitions(String systemName, String systemType,
                                                                     String systemObject, String importFile,
                                                                     ValidateFieldDefinitionsRequest validateRequest) throws Exception {

        validateFieldDefinitionRequestParameters("Validate", systemName, systemType, systemObject, importFile);

        // get default spec from s3
        if (validateRequest.getImportWorkflowSpec() == null || MapUtils.isEmpty(validateRequest.getImportWorkflowSpec().getFieldDefinitionsRecordsMap())) {
            throw new RuntimeException(String.format("no spec info for system type %s and system object %s",
                    systemType, systemObject));
        }

        if (MapUtils.isEmpty(validateRequest.getAutodetectionResultsMap())) {
            throw new RuntimeException("no auto-detected field definition");
        }

        if (validateRequest.getCurrentFieldDefinitionsRecord() == null || MapUtils.isEmpty(validateRequest.getCurrentFieldDefinitionsRecord().getFieldDefinitionsRecordsMap())) {
            throw new RuntimeException("no field definition records");
        }

        Map<String, FieldDefinition> autoDetectionResultsMap = validateRequest.getAutodetectionResultsMap();
        Map<String, List<FieldDefinition>> specFieldDefinitionsRecordsMap =
                validateRequest.getImportWorkflowSpec().getFieldDefinitionsRecordsMap();
        Map<String, List<FieldDefinition>> fieldDefinitionsRecordsMap =
                validateRequest.getCurrentFieldDefinitionsRecord().getFieldDefinitionsRecordsMap();
        Map<String, FieldDefinition> existingFieldDefinitionMap = validateRequest.getExistingFieldDefinitionsMap();
        Map<String, OtherTemplateData> otherTemplateDataMap = validateRequest.getOtherTemplateDataMap();

        // 1 Generate source file and resolver
        SourceFile sourceFile = getSourceFile(importFile);
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);

        // 2. generate validation message
        ValidateFieldDefinitionsResponse response =
                ImportWorkflowUtils.generateValidationResponse(fieldDefinitionsRecordsMap, autoDetectionResultsMap,
                        specFieldDefinitionsRecordsMap, existingFieldDefinitionMap, otherTemplateDataMap, resolver);
        // set field definition records map for ui
        response.setFieldDefinitionsRecordsMap(fieldDefinitionsRecordsMap);
        return response;
    }


    /**
     * 1. Basic Validation of Individual Spec
     *  Must be valid JSON.
     *  Must be deserializable into valid ImportWorkflowSpec.
     *  All required fields are set for each Attribute
     *  Can't have two FieldDefinitions with the same fieldNames.
     *  Make sure no duplicates in matchingColumnNames.
     * 2. Spec must be validated against prior version for same System Type and System Object
     *  Physical type of a FieldDefinition with same fieldName cannot be changed in later Specs, at least if there
     *  exists tenant data for this Spec.
     * 3. Specs for the same System Object across System Types must be consistent.
     *  Physical type of a FieldDefinition with the same fieldName cannot vary across Specs for the same System Object.
     * @param systemType
     * @param systemObject
     * @param specInputStream
     * @return
     * @throws Exception
     */
    @Override
    public List<String> validateIndividualSpec(String systemType, String systemObject,
                                                     InputStream specInputStream) throws Exception {

        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();

        ImportWorkflowSpec existingSpec = importWorkflowSpecProxy.getImportWorkflowSpec(customerSpace.toString(),
                systemType,
                systemObject);
        List<ImportWorkflowSpec> specList =
                importWorkflowSpecProxy.getSpecWithSameObjectExcludeTypeFromS3(customerSpace.toString(), systemType,
                        systemObject);
        Map<String, Map<String, FieldDefinition>> specWithSameObjectMap = new HashMap<>();
        specList.forEach(spec ->
            specWithSameObjectMap.put(spec.getSystemType(),
            spec.getFieldDefinitionsRecordsMap().values().stream().flatMap(List::stream).collect(Collectors.toMap(FieldDefinition::getFieldName, e -> e))));

        List<String> errors = new ArrayList<>();
        Map<String, List<FieldDefinition>> currentFieldDefinitionsMap;
        if (existingSpec == null) {
            currentFieldDefinitionsMap = new HashMap<>();
        } else {
            currentFieldDefinitionsMap = existingSpec.getFieldDefinitionsRecordsMap();
        }
        Map<String, FieldDefinition> fieldNameToDefinition =
                currentFieldDefinitionsMap.values().stream().flatMap(List::stream).collect(Collectors.toMap(FieldDefinition::getFieldName, e -> e));
        ImportWorkflowSpec importSpec = null;
        if (specInputStream != null) {
            try {
                importSpec = JsonUtils.deserialize(specInputStream, ImportWorkflowSpec.class);
            } catch (Exception e) {
                errors.add("input file can't be deserializable into valid ImportWorkflowSpec.");
            }
            Preconditions.checkNotNull(importSpec);
            if (importSpec.equals(existingSpec)) {
                errors.add(String.format("input spec matches the existing spec with system type %s and system object " +
                        "%s", systemType, systemObject));
            }
            Map<String, List<FieldDefinition>> inputFieldDefinitionMap = importSpec.getFieldDefinitionsRecordsMap();
            Preconditions.checkNotNull(inputFieldDefinitionMap);
            Set<String> fieldNameSet = new HashSet<>();
            for (Map.Entry<String, List<FieldDefinition>> entry : inputFieldDefinitionMap.entrySet()) {
                List<FieldDefinition> definitions = entry.getValue();
                for (FieldDefinition definition : definitions) {
                    String fieldName = definition.getFieldName();
                    // check duplicates for definition
                    if (StringUtils.isEmpty(fieldName)) {
                        errors.add("empty field name found");
                    } else if (!fieldNameSet.add(definition.getFieldName())) {
                        // duplicate
                        errors.add(String.format("field definitions have same field name %s", fieldName));
                    }
                    // check duplicates for matching column name
                    List<String> matchingColumnNames = definition.getMatchingColumnNames();
                    if (CollectionUtils.isEmpty(matchingColumnNames)) {
                        errors.add(String.format("empty matching columns for field %s", fieldName));
                    } else {
                        Set<String> columnNamesSet = new HashSet<>();
                        for (String name : matchingColumnNames) {
                            if (!columnNamesSet.add(name)) {
                                // duplicate
                                errors.add(String.format("duplicates found in matching column for field name %s", fieldName));
                            }
                        }
                    }
                    // check required flag
                    if (definition.isRequired() == null) {
                        errors.add(String.format("required flag should be set for %s", fieldName));
                    }
                    // check field type
                    if (definition.getFieldType() == null) {
                        errors.add(String.format("field type is empty for field %s", fieldName));
                    }
                    FieldDefinition existingDefinition = fieldNameToDefinition.get(fieldName);
                    if (existingDefinition != null && definition.getFieldType() != existingDefinition.getFieldType()) {
                        // error out
                        errors.add(String.format("Physical type %s of the FieldDefinition with " +
                                "same field name %s cannot be changed to %s for system type %s and system object " +
                                        "%s",
                                existingDefinition.getFieldType(),
                                fieldName, definition.getFieldType(),
                                systemType, systemObject));
                    }
                    for (Map.Entry<String, Map<String, FieldDefinition>> specEntry :
                            specWithSameObjectMap.entrySet()) {
                        Map<String, FieldDefinition> specMap = specEntry.getValue();
                        FieldDefinition specDefinition = specMap.get(fieldName);
                        if (specDefinition != null && definition.getFieldType() != specDefinition.getFieldType()) {
                            // error out
                            errors.add(String.format("Physical type %s of the FieldDefinition with " +
                                            "same field name %s cannot be changed to %s for system type %s and " +
                                            "system object %s",
                                    specDefinition.getFieldType(),
                                    fieldName, definition.getFieldType(),
                                    specEntry.getKey().toLowerCase(), systemObject));
                        }
                    }
                }
            }
        }

        return errors;
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

    private static void printDataFeedTask(String tableType, DataFeedTask dataFeedTask) {
        log.info("Print DataFeedTask containing " + tableType);
        if (dataFeedTask == null) {
            log.info("$JAW$ dataFeedTask is null");
            return;
        }
        printTableAttributes(tableType, dataFeedTask.getImportTemplate());
    }

    private static void printTableAttributes(String tableType, Table table) {
        if (table == null) {
            log.info("\n{} Table is null");
        }
        log.info("\n{} - Table name {} (display name {}) contains the following Attributes:\n", tableType,
                table.getName(), table.getDisplayName());
        List<Attribute> attributes = table.getAttributes();
        if (CollectionUtils.isNotEmpty(attributes)) {
            for (Attribute attribute : attributes) {
                log.info("   Name: " + attribute.getName() + "  Physical Data Type: " +
                        attribute.getPhysicalDataType() + "  DisplayName: " + attribute.getDisplayName());
            }
        }
    }


}

