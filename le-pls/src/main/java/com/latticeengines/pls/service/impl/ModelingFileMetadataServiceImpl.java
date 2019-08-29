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
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation.ValidationStatus;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.RequiredType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.validation.ReservedField;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.ImportWorkflowSpecService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.EntityMatchGAConverterUtils;
import com.latticeengines.pls.util.ImportWorkflowUtils;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
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
    private ImportWorkflowSpecService importWorkflowSpecService;

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
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entity);
        if (sourceFile.getSchemaInterpretation() != schemaInterpretation) {
            sourceFile.setSchemaInterpretation(schemaInterpretation);
            sourceFileService.update(sourceFile);
        }
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        Table table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                batonService.isEntityMatchEnabled(customerSpace));
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
        EntityMatchGAConverterUtils.convertGuessingMappings(enableEntityMatch, enableEntityMatchGA, resultDocument);
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
        for(FieldMapping bestEffortMapping : documentBestEffort.getFieldMappings()) {
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
                        boolean match = resolver.checkUserDateType(fieldMapping, warningMessage, formatWithBestEffort);
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
        if (dataFeedTask == null) {
            table = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                    batonService.isEntityMatchEnabled(customerSpace));
            regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity), feedType, null);
            EntityMatchGAConverterUtils.convertSavingMappings(enableEntityMatch, enableEntityMatchGA, fieldMappingDocument);
        } else {
            table = dataFeedTask.getImportTemplate();
            regulateFieldMapping(fieldMappingDocument, BusinessEntity.getByName(entity), feedType, table);
            if (table.getAttribute(InterfaceName.AccountId) == null) {
                EntityMatchGAConverterUtils.convertSavingMappings(enableEntityMatch, enableEntityMatchGA, fieldMappingDocument);
            }
        }
        schemaTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId,
                batonService.isEntityMatchEnabled(customerSpace));
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
                if (fieldMapping.getIdType() != null) {
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

    private void setSystemIdMapping(CustomerSpace customerSpace, String feedType,
                                    List<FieldMapping> customerLatticeIdList, FieldMapping fieldMapping) {
        String systemName = cdlService.getSystemNameFromFeedType(feedType);
        EntityType entityType = EntityType.matchFeedType(feedType);
        if (StringUtils.isNotEmpty(systemName)) {
            if (StringUtils.isEmpty(fieldMapping.getSystemName()) || systemName.equals(fieldMapping.getSystemName())) { // Set field as current system id
                fieldMapping.setFieldType(UserDefinedType.TEXT);
                S3ImportSystem importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                switch (fieldMapping.getIdType()) {
                    case Account:
                        String accountSystemId;
                        boolean primaryAccount;
                        if (entityType == null || !BusinessEntity.Account.equals(entityType.getEntity())
                                || importSystem.getSystemType().getPrimaryAccount().equals(entityType)) {
                            accountSystemId = importSystem.getAccountSystemId();
                            primaryAccount = true;
                        } else {
                            accountSystemId = importSystem.getSecondaryAccountId(entityType);
                            primaryAccount = false;
                        }
                        if (fieldMapping.isMapToLatticeId() && primaryAccount) {
                            importSystem.setMapToLatticeAccount(true);
                            cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                            importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                        }
                        if (StringUtils.isEmpty(accountSystemId)) {
                            accountSystemId = importSystem.generateAccountSystemId();
                            if (primaryAccount) {
                                importSystem.setAccountSystemId(accountSystemId);
                                importSystem.setMapToLatticeAccount(fieldMapping.isMapToLatticeId());
                            } else {
                                importSystem.addSecondaryAccountId(entityType, accountSystemId);
                            }
                            cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                            fieldMapping.setMappedToLatticeField(false);
                            fieldMapping.setMappedField(accountSystemId);
                        } else {
                            fieldMapping.setMappedToLatticeField(accountSystemId.equals(fieldMapping.getMappedField()));
                            fieldMapping.setMappedField(accountSystemId);
                        }
                        if (importSystem.isMapToLatticeAccount() && primaryAccount) {
                            FieldMapping customerLatticeId = new FieldMapping();
                            customerLatticeId.setUserField(fieldMapping.getUserField());
                            customerLatticeId.setMappedField(InterfaceName.CustomerAccountId.name());
                            customerLatticeId.setFieldType(fieldMapping.getFieldType());
                            customerLatticeIdList.add(customerLatticeId);
                        }
                        break;
                    case Contact:
                        String contactSystemId;
                        boolean primaryContact;
                        if (entityType == null || !BusinessEntity.Contact.equals(entityType.getEntity())
                                || importSystem.getSystemType().getPrimaryContact().equals(entityType)) {
                            contactSystemId = importSystem.getContactSystemId();
                            primaryContact = true;
                        } else {
                            contactSystemId = importSystem.getSecondaryContactId(entityType);
                            primaryContact = false;
                        }
                        if (fieldMapping.isMapToLatticeId() && primaryContact) {
                            importSystem.setMapToLatticeContact(true);
                            cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                            importSystem = cdlService.getS3ImportSystem(customerSpace.toString(), systemName);
                        }
                        if (StringUtils.isEmpty(contactSystemId)) {
                            contactSystemId = importSystem.generateContactSystemId();
                            if (primaryContact) {
                                importSystem.setContactSystemId(contactSystemId);
                                importSystem.setMapToLatticeContact(fieldMapping.isMapToLatticeId());
                            } else {
                                importSystem.addSecondaryContactId(entityType, contactSystemId);
                            }
                            cdlService.updateS3ImportSystem(customerSpace.toString(), importSystem);
                            fieldMapping.setMappedToLatticeField(false);
                            fieldMapping.setMappedField(contactSystemId);
                        }
                        else {
                            fieldMapping.setMappedToLatticeField(contactSystemId.equals(fieldMapping.getMappedField()));
                            fieldMapping.setMappedField(contactSystemId);
                        }
                        if (importSystem.isMapToLatticeContact() && primaryContact) {
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
                    default:
                        throw new IllegalArgumentException("Unrecognized idType: " + fieldMapping.getIdType());
                }
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
        if (BusinessEntity.Account.name().equals(entity)) {
            int limit =
                    appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(MultiTenantContext.getShortTenantId(), DataLicense.ACCOUNT.getDataLicense());
            ValidateFileHeaderUtils.checkForHeaderNum(headerFields, limit);
        } else if (BusinessEntity.Contact.equals(entity)) {
            int limit =
                    appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(MultiTenantContext.getShortTenantId(), DataLicense.CONTACT.getDataLicense());
            ValidateFileHeaderUtils.checkForHeaderNum(headerFields, limit);
        }

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
    public FieldDefinitionsRecord fetchFieldDefinitions(String systemName, String systemType,
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
        // TODO(jwinter): Make sure the implemented approach is correct.
        String feedType = ImportWorkflowUtils.getFeedTypeFromSystemNameAndEntityType(systemName, entityType);

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

        //log.info(String.format("Parameter Values:\nsystemName: %s\nsystemType: %s\nsystemObject: %s" +
        //                "\nimportFile: %s", systemName, systemType, systemObject, importFile));
        log.info(String.format("Internal Values:\n   entity: %s\n   subType: %s\n   feedType: %s\n   source: %s\n" +
                "   Source File: %s\n   Customer Space: %s", entityType.getEntity(), entityType.getSubType(), feedType,
                source, sourceFile.getName(), customerSpace.toString()));

        // 3. Get flags relevant to import workflow.
        // TODO(jwinter): Figure out how to incorporate all the system flags for entity match.
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);

        // TODO(jwinter): Need to incorporate Batch Store into initial generation of FieldDefinitionsRecord.
        // 4. Generate FetchFieldMappingResponse by combining:
        //    a. Spec for this system.
        //    b. Existing field definitions from DataFeedTask.
        //    c. Columns from the sourceFile.
        //    d. Batch Store (TODO)

        // 4a. Retrieve Spec for given systemType and systemObject.
        ImportWorkflowSpec importWorkflowSpec = importWorkflowSpecService.loadSpecFromS3(systemType, systemObject);

        // 4b. Find previously saved template matching this customerSpace, source, feedType, and entityType, if it
        // exists.
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType,
                entityType.getEntity().name());
        printDataFeedTask("Existing DataFeedTask Template", dataFeedTask);
        Table existingTable = null;
        if (dataFeedTask != null) {
            existingTable = dataFeedTask.getImportTemplate();
        }

        // 4c. Create a MetadataResolver using the sourceFile.
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);

        // TODO(jwinter):  Understand exactly what is needed from batch store...

        // 4d. Generate the initial FieldMappingsRecord based on the Spec, existing table, input file, and batch store.
        FieldDefinitionsRecord fieldDefinitionsRecord =
                ImportWorkflowUtils.createFieldDefinitionsRecordFromSpecAndTable(importWorkflowSpec, existingTable,
                        resolver);


        log.info("JAW ------ END Real Fetch Field Definition -----");

        return fieldDefinitionsRecord;
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
    public FieldDefinitionsRecord commitFieldDefinitions(String systemName, String systemType,
                                                                 String systemObject, String importFile,
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
        // TODO(jwinter): Make sure the implemented approach is correct.
        String feedType = ImportWorkflowUtils.getFeedTypeFromSystemNameAndEntityType(systemName, entityType);

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

        // 2g. Add tenant to MultiTenantContext.
        // TODO(jwinter): Do we need this?
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("Cannot find the tenant %s", customerSpace.getTenantId()));
        }
        MultiTenantContext.setTenant(tenant);

        //log.info(String.format("Parameter Values:\nsystemName: %s\nsystemType: %s\nsystemObject: %s" +
        //        "\nimportFile: %s", systemName, systemType, systemObject, importFile));
        log.info(String.format("Internal Values:\n   entity: %s\n   subType: %s\n   feedType: %s\n   source: %s\n" +
                        "   Source File: %s\n   Customer Space: %s\n   Tenant: %s", entityType.getEntity(),
                entityType.getSubType(), feedType, source, sourceFile.getName(), customerSpace.toString(),
                tenant.getId()));

        // 3. Generate new table from FieldDefinitionsRecord,
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
        Table newTable = ImportWorkflowUtils.getTableFromFieldDefinitionsRecord(commitRequest, false);

        // 4. Delete old table associated with the source file from the database if it exists.
        if (sourceFile.getTableName() != null) {
            metadataProxy.deleteTable(customerSpace.toString(), sourceFile.getTableName());
        }

        // 5. Associate the new table with the source file and add new table to the database.
        // TODO(jwinter): Figure out if the prefex should always be "SourceFile".
        newTable.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
        // TODO(jwinter): Figure out how to properly set the Table display name.
        newTable.setDisplayName(newTable.getName());
        log.info("New table name/display name is: " + newTable.getName());
        if (newTable.getTenant() == null) {
            newTable.setTenant(MultiTenantContext.getTenant());
            log.info("Setting new table tenant to: "+ newTable.getTenant().getName());
        }
        if (newTable.getTenantId() == null) {
            newTable.setTenantId(MultiTenantContext.getTenant().getPid());
            log.info("Setting new table tenant PID to: "+ newTable.getTenant().getPid());
        }
        printTableAttributes("New Table", newTable);
        log.info("New Table is:\n" + JsonUtils.pprint(newTable));


        metadataProxy.createTable(customerSpace.toString(), newTable.getName(), newTable);
        sourceFile.setTableName(newTable.getName());
        sourceFileService.update(sourceFile);

        // 6. Update the CDL External System data structures.
        // TODO(jwinter): Complete this work.
        // Set external system column name
        if (BusinessEntity.Account.equals(entityType.getEntity()) ||
                BusinessEntity.Contact.equals(entityType.getEntity())) {
            setCDLExternalSystem(resolver.getExternalSystem(), entityType.getEntity());
        }

        // TODO(jwinter): Can we skip checking for an existing table, checking if the tables are identical, and
        // copying the changes into the existing table and just write the new table back?  The question is whether there
        // are fields and properties bucket values that are not related to import that have been stored in the
        // existing table's attributes?

        // 7. Create or Update the DataFeedTask
        // a. If there is an existing DataFeedTask template table.
        //   i. If the tables are identical with respect to data updated during import workflow template setup (name,
        //      displayName, physicalDataType, data/time formats, and timezone), there is no more steps to do.
        //   ii. Merge the new table and current DataFeedTask template table, adding new attributes and updating
        //       display name and data formats of old attributes.
        //   iii. Update the DataFeedTask with the new table.
        // b. If there is no existing DataFeedTask template table.
        //   i. Create a new DataFeedTask containing the new table.
        //   ii. Update the DataFeed status.
        // c. Update Attribute Configs.
        // d. Send email about S3 update.


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
            // TODO(jwinter): Figure out how to get subtype from incoming parameters.
            dataFeedTask.setSubType(entityType.getSubType());
            dataFeedTask.setTemplateDisplayName(entityType.getDefaultFeedTypeName());
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            // TODO(jwinter): Add DropBoxService stuff.

            DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
            if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
                dataFeedProxy.updateDataFeedStatus(customerSpace.toString(), DataFeed.Status.Initialized.getName());
            }
        }
        // TODO(jwinter): Do we need to add code to update the Attr Configs?

        // TODO(jwinter): Add code to send email about S3 Template change.


        // TODO(jwinter): Add flag to indicate a workflow job should be submitted and the code for submitting
        // workflow jobs.
        // 8. If requested, submit a workflow import job for this new template.

        FieldDefinitionsRecord commitResponse = new FieldDefinitionsRecord();
        // TODO(jwinter): FieldDefinitionsRecord should reflect any changes when new table is merged with
        // existing table.
        commitResponse.setFieldDefinitionsRecordsMap(commitRequest.getFieldDefinitionsRecordsMap());
        log.info("JAW ------ END Real Commit Field Definition -----");

        return commitResponse;
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
