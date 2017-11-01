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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
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
import com.latticeengines.domain.exposed.validation.ReservedField;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
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
        List<Attribute> matchingAttributes = SchemaRepository.instance()
                .matchingAttributes(getSchemaInterpretation(modelId));
        matchingAttributes.removeIf(matchingAttr -> requiredAttributes.stream()
                .anyMatch(requiredAttr -> requiredAttr.getInterfaceName() == matchingAttr.getInterfaceName()));
        requiredAttributes.addAll(matchingAttributes);

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

        List<Attribute> requiredAttributes = modelMetadataService.getRequiredColumns(modelId);
        List<Attribute> matchingAttributes = SchemaRepository.instance()
                .matchingAttributes(getSchemaInterpretation(modelId));
        matchingAttributes
                .removeIf(matchingAttr -> fieldMappingDocument.getIgnoredFields().contains(matchingAttr.getName())
                        || requiredAttributes.stream().anyMatch(
                                requiredAttr -> requiredAttr.getInterfaceName() == matchingAttr.getInterfaceName()));

        List<Attribute> modelAttributes = new ArrayList<>(matchingAttributes);
        modelAttributes.addAll(requiredAttributes);

        SourceFile sourceFile = sourceFileService.findByName(csvFileName);
        resolveModelAttributeBasedOnFieldMapping(modelAttributes, fieldMappingDocument);

        Table table = createTableFromMetadata(modelAttributes, sourceFile);
        MetadataResolver resolver = new MetadataResolver(sourceFile.getPath(), yarnConfiguration, null);
        resolver.sortAttributesBasedOnSourceFileSequence(table);

        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
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

    @VisibleForTesting
    void resolveModelAttributeBasedOnFieldMapping(List<Attribute> modelAttributes,
            FieldMappingDocument fieldMappingDocument) {
        log.info(String.format("Before resolving attributes, the model attributes are: %s",
                Arrays.toString(modelAttributes.toArray())));
        log.info(String.format("The field mapping list is: %s",
                Arrays.toString(fieldMappingDocument.getFieldMappings().toArray())));

        // Overwrite attributes displayname that can be mapped to fields in
        // required columns
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
                }
            }
        }

        // Create attributes based on fieldNames for those unmapped fields
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.isMappedToLatticeField() || fieldMapping.getMappedField() != null) {
                log.warn(String.format("Something wrong with field mapping, Field %s should be mapped to %s already",
                        fieldMapping.getUserField(), fieldMapping.getMappedField()));
                continue;
            }
            String unmappedScoringHeader = fieldMapping.getUserField();
            int len = unmappedScoringHeader.length();
            int version = 0;
            StringBuilder sb = new StringBuilder(unmappedScoringHeader);
            for (Attribute modelAttribute : modelAttributes) {
                if (isScoringFieldMatchedWithModelAttribute(sb.toString(), modelAttribute)) {
                    log.warn(String.format(
                            "Protential conflict between scoring header %s and model attribute displayname %s",
                            sb.toString(), modelAttribute.getDisplayName()));
                    sb.setLength(len);
                    sb.append(String.format("_%d", ++version));
                }
            }
            modelAttributes.add(getAttributeFromFieldName(sb.toString(), fieldMapping.getMappedField()));
        }

        modelAttributes.stream().filter(attr -> fieldMappingDocument.getIgnoredFields().contains(attr.getDisplayName()))
                .forEach(attr -> attr.getApprovedUsage().add(ApprovedUsage.IGNORED.toString()));

        log.info(String.format("After resolving attributes, the model attributes are: %s", modelAttributes));
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
