package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
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
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SchemaInterpretationFunctionalInterface;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.domain.exposed.validation.ReservedField;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.PlsFeatureFlagService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("scoringFileMetadataService")
public class ScoringFileMetadataServiceImpl implements ScoringFileMetadataService {

    private static final Logger log = LoggerFactory.getLogger(ScoringFileMetadataServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private PlsFeatureFlagService plsFeatureFlagService;

    @Override
    public InputStream validateHeaderFields(InputStream stream, CloseableResourcePool closeableResourcePool,
            String displayName) {
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(ValidateFileHeaderUtils.BIT_PER_BYTE * ValidateFileHeaderUtils.BYTE_NUM);
        Set<String> headerFields = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        try {
            stream.reset();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        ValidateFileHeaderUtils.checkForEmptyHeaders(displayName, headerFields);
        Collection<String> reservedWords = Arrays
                .asList(new String[] { ReservedField.Percentile.displayName, ReservedField.Rating.displayName });
        ValidateFileHeaderUtils.checkForReservedHeaders(displayName, headerFields, reservedWords);
        return stream;
    }

    @Override
    public FieldMappingDocument mapRequiredFieldsWithFileHeaders(String csvFileName, String modelId) {
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
        SchemaInterpretation schemaInterpretation = getSchemaInterpretation(modelId);

        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setFieldMappings(new ArrayList<FieldMapping>());
        fieldMappingDocument.setIgnoredFields(new ArrayList<String>());

        fieldMappingDocument.getRequiredFields().add(InterfaceName.Id.name());
        if (!modelSummary.getModelSummaryConfiguration().getBoolean(ProvenancePropertyName.ExcludePropdataColumns)
                && !plsFeatureFlagService.isFuzzyMatchEnabled()) {
            SchemaInterpretationFunctionalInterface function = (interfaceName) -> fieldMappingDocument
                    .getRequiredFields().add(interfaceName.name());
            schemaInterpretation.apply(function);
        }

        Set<String> scoringHeaderFields = getHeaderFields(csvFileName);
        List<Attribute> requiredAttributes = modelMetadataService.getRequiredColumns(modelId);
        List<Attribute> schemaFields = SchemaRepository.instance().getSchema(schemaInterpretation).getAttributes();
        Iterator<String> scoringHeaderFieldsIterator = scoringHeaderFields.iterator();
        while (scoringHeaderFieldsIterator.hasNext()) {
            String scoringHeaderField = scoringHeaderFieldsIterator.next();
            FieldMapping fieldMapping = new FieldMapping();

            Iterator<Attribute> requiredAttributesIterator = requiredAttributes.iterator();
            while (requiredAttributesIterator.hasNext()) {
                Attribute requiredAttribute = requiredAttributesIterator.next();

                if (isScoringFieldMatchedWithModelAttribute(scoringHeaderField, requiredAttribute)) {
                    fieldMapping.setUserField(scoringHeaderField);
                    fieldMapping.setMappedField(requiredAttribute.getName());
                    fieldMapping.setMappedToLatticeField(true);
                    fieldMappingDocument.getFieldMappings().add(fieldMapping);

                    scoringHeaderFieldsIterator.remove();
                    requiredAttributesIterator.remove();
                    break;
                }
            }
            if (!fieldMapping.isMappedToLatticeField()) {
                Iterator<Attribute> schemaFieldsIterator = schemaFields.iterator();
                while (schemaFieldsIterator.hasNext()) {
                    Attribute schemaField = schemaFieldsIterator.next();
                    if (isScoringFieldMatchedWithModelAttribute(scoringHeaderField, schemaField)) {
                        fieldMapping.setUserField(scoringHeaderField);
                        fieldMapping.setMappedField(schemaField.getName());
                        // fields here are not in model event table, so we set
                        // false here
                        fieldMapping.setMappedToLatticeField(false);
                        fieldMappingDocument.getFieldMappings().add(fieldMapping);

                        scoringHeaderFieldsIterator.remove();
                        schemaFieldsIterator.remove();
                        break;
                    }
                }
            }
        }

        for (Attribute requiredAttribute : requiredAttributes) {
            FieldMapping fieldMapping = new FieldMapping();
            fieldMapping.setMappedField(requiredAttribute.getName());
            fieldMapping.setMappedToLatticeField(true);
            fieldMappingDocument.getFieldMappings().add(fieldMapping);
        }

        for (String scoringHeaderField : scoringHeaderFields) {
            FieldMapping fieldMapping = new FieldMapping();
            fieldMapping.setUserField(scoringHeaderField);
            fieldMapping.setMappedToLatticeField(false);
            fieldMapping.setFieldType(UserDefinedType.TEXT);
            fieldMappingDocument.getFieldMappings().add(fieldMapping);
        }

        removeUnwantedFieldsFromFieldMappingDocuments(fieldMappingDocument, schemaInterpretation);
        return fieldMappingDocument;
    }

    private SchemaInterpretation getSchemaInterpretation(String modelId) {
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
        if (modelSummary == null) {
            throw new RuntimeException(String.format("No such model summary with id %s", modelId));
        }
        String schemaInterpretationStr = modelSummary.getSourceSchemaInterpretation();
        if (schemaInterpretationStr == null) {
            throw new LedpException(LedpCode.LEDP_18087, new String[] { schemaInterpretationStr });
        }
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.valueOf(schemaInterpretationStr);
        return schemaInterpretation;
    }

    @Override
    public Table saveFieldMappingDocument(String csvFileName, String modelId,
            FieldMappingDocument fieldMappingDocument) {
        SchemaInterpretation schemaInterpretation = getSchemaInterpretation(modelId);
        List<Attribute> schemaFields = SchemaRepository.instance().getSchema(schemaInterpretation).getAttributes();

        List<Attribute> modelAttributes = modelMetadataService.getRequiredColumns(modelId);
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);

        SourceFile sourceFile = sourceFileService.findByName(csvFileName);
        resolveModelAttributeBasedOnFieldMapping(modelAttributes, schemaFields, fieldMappingDocument);
        Table table = createTableFromMetadata(modelAttributes, sourceFile);
        table.deduplicateAttributeNames(modelMetadataService.getLatticeAttributeNames(modelId));

        if (plsFeatureFlagService.isFuzzyMatchEnabled()) {
            SchemaInterpretationFunctionalInterface function = (interfaceName) -> {
                Attribute domainAttribute = table.getAttribute(interfaceName);
                if (domainAttribute != null) {
                    domainAttribute.setNullable(Boolean.TRUE);
                }
            };
            SchemaInterpretation.valueOf(modelSummary.getSourceSchemaInterpretation()).apply(function);
        }

        Tenant tenant = MultiTenantContext.getTenant();
        metadataProxy.createTable(tenant.getId(), table.getName(), table);

        sourceFile.setTableName(table.getName());
        sourceFileService.update(sourceFile);
        return table;
    }

    @Override
    public Set<String> getHeaderFields(String csvFileName) {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        String filePath = sourceFileService.findByName(csvFileName).getPath();
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            InputStream is = fs.open(new Path(filePath));
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

    private void removeUnwantedFieldsFromFieldMappingDocuments(FieldMappingDocument fieldMappingDocument,
            SchemaInterpretation schemaInterpretation) {
        Iterator<FieldMapping> fieldMappingIterator = fieldMappingDocument.getFieldMappings().iterator();
        Table schemaTable = SchemaRepository.instance().getSchema(schemaInterpretation);

        while (fieldMappingIterator.hasNext()) {
            FieldMapping fieldMapping = fieldMappingIterator.next();
            if (schemaTable.getAttribute(fieldMapping.getMappedField()) != null
                    && LogicalDataType.isExcludedFromScoringFileMapping(
                            schemaTable.getAttribute(fieldMapping.getMappedField()).getLogicalDataType())) {
                fieldMappingIterator.remove();
            }
        }
    }

    @VisibleForTesting
    void resolveModelAttributeBasedOnFieldMapping(List<Attribute> modelAttributes, List<Attribute> schemaFields,
            FieldMappingDocument fieldMappingDocument) {
        log.info(String.format("Before resolving attributes, the model attributes are: %s",
                Arrays.toString(modelAttributes.toArray())));
        log.info(String.format("The field mapping list is: %s",
                Arrays.toString(fieldMappingDocument.getFieldMappings().toArray())));

        Set<String> modelAttributeNames = modelAttributes.stream().map(modelAttribute -> modelAttribute.getName())
                .collect(Collectors.toSet());
        // Overwrite attributes that can be mapped to fields in required columns
        Iterator<Attribute> attrIterator = modelAttributes.iterator();
        while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            Iterator<FieldMapping> fieldMappingIterator = fieldMappingDocument.getFieldMappings().iterator();
            while (fieldMappingIterator.hasNext()) {
                FieldMapping fieldMapping = fieldMappingIterator.next();
                if (fieldMapping.isMappedToLatticeField()
                        && fieldMapping.getMappedField().equals(attribute.getName())) {
                    attribute.setDisplayName(fieldMapping.getUserField());
                    fieldMappingIterator.remove();
                    break;
                } else if (!fieldMapping.isMappedToLatticeField() //
                        && fieldMapping.getMappedField() != null //
                        && fieldMapping.getMappedField().equals(attribute.getName()) //
                        && LogicalDataType.isExcludedFromScoringFileMapping(attribute.getLogicalDataType())) {
                    attribute.setDisplayName(fieldMapping.getUserField());
                    fieldMappingIterator.remove();
                    break;
                }
            }
        }

        // Create attributes that can be mapped to fields in schema columns
        attrIterator = schemaFields.iterator();
        while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            Iterator<FieldMapping> fieldMappingIterator = fieldMappingDocument.getFieldMappings().iterator();
            while (fieldMappingIterator.hasNext()) {
                FieldMapping fieldMapping = fieldMappingIterator.next();
                if (fieldMapping.isMappedToLatticeField()
                        && fieldMapping.getMappedField().equals(attribute.getName())) {
                    Attribute attr = new Attribute();
                    AttributeUtils.copyPropertiesFromAttribute(attribute, attr, false);
                    attr.setDisplayName(fieldMapping.getUserField());
                    modelAttributes.add(attr);
                    fieldMappingIterator.remove();
                    break;
                }
            }
        }

        // Create attributes based on fieldNames for those unmapped fields
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (!fieldMapping.isMappedToLatticeField()
                    || !modelAttributeNames.contains(fieldMapping.getMappedField())) {
                if (fieldMapping.getUserField() == null) {
                    continue;
                }
                String unmappedScoringHeader = fieldMapping.getUserField();
                for (Attribute modelAttribute : modelAttributes) {
                    if (isScoringFieldMatchedWithModelAttribute(unmappedScoringHeader, modelAttribute)) {
                        // unmappedScoringHeader =
                        // unmappedScoringHeader.concat("_1");
                        log.warn(String.format(
                                "Protential conflict between scoring header %s and model attribute displayname %s",
                                unmappedScoringHeader, modelAttribute.getDisplayName()));
                        // we may need to throw exception here as renaming
                        // should not work
                    }
                }

                modelAttributes.add(getAttributeFromFieldName(unmappedScoringHeader, fieldMapping.getMappedField()));
            }
        }

        if (fieldMappingDocument.getIgnoredFields() != null) {
            for (final String ignoredField : fieldMappingDocument.getIgnoredFields()) {
                if (ignoredField != null) {
                    Attribute attribute = Iterables.find(modelAttributes, new Predicate<Attribute>() {
                        @Override
                        public boolean apply(@Nullable Attribute input) {
                            return ignoredField.equals(input.getDisplayName());
                        }
                    }, null);
                    if (attribute != null) {
                        log.info(String.format("Current ignored fileds include %s.", attribute.getDisplayName()));
                        attribute.setApprovedUsage(ApprovedUsage.NONE, ApprovedUsage.IGNORED);
                    }
                }
            }
        }
        log.info(String.format("After resolving attributes, the model attributes are: %s",
                Arrays.toString(modelAttributes.toArray())));
    }

    private Attribute getAttributeFromFieldName(String scoringHeaderName, String fieldName) {
        Attribute attribute = new Attribute();

        attribute.setName(fieldName == null
                ? ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(scoringHeaderName) : fieldName);
        attribute.setPhysicalDataType(FieldType.STRING.toString().toLowerCase());
        attribute.setDisplayName(scoringHeaderName);
        attribute.setApprovedUsage(ApprovedUsage.NONE.name());
        attribute.setCategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        attribute.setFundamentalType(ModelingMetadata.FT_ALPHA);
        attribute.setStatisticalType(ModelingMetadata.NOMINAL_STAT_TYPE);
        attribute.setNullable(true);
        attribute.setTags(ModelingMetadata.INTERNAL_TAG);

        return attribute;
    }

    private Table createTableFromMetadata(List<Attribute> attributes, SourceFile sourceFile) {
        Table table = new Table();

        table.setPrimaryKey(null);
        table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
        table.setDisplayName(sourceFile.getDisplayName());
        table.setAttributes(attributes);
        Attribute lastModified = table.getAttribute(InterfaceName.LastModifiedDate);
        if (lastModified == null) {
            table.setLastModifiedKey(null);
        }
        table.deduplicateAttributeNames();

        return table;
    }

    private boolean isScoringFieldMatchedWithModelAttribute(String scoringField, Attribute modelAttribute) {
        List<String> allowedDisplayNames = modelAttribute.getAllowedDisplayNames();
        if (allowedDisplayNames != null) {
            for (int i = 0; i < modelAttribute.getAllowedDisplayNames().size(); i++) {
                if (allowedDisplayNames.get(i).equalsIgnoreCase(scoringField)) {
                    return true;
                }
            }
        }

        if (modelAttribute.getDisplayName().equalsIgnoreCase(scoringField)
                || modelAttribute.getName().equalsIgnoreCase(scoringField)) {
            return true;
        }
        return false;
    }

}
