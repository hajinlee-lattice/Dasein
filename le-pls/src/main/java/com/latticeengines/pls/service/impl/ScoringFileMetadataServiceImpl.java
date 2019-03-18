package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
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
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.PlsFeatureFlagService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("scoringFileMetadataService")
public class ScoringFileMetadataServiceImpl implements ScoringFileMetadataService {

    private static final Logger log = LoggerFactory.getLogger(ScoringFileMetadataServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private PlsFeatureFlagService plsFeatureFlagService;

    @Autowired
    private BatonService batonService;

    @Autowired
    private AttributeService attributeService;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

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
        ValidateFileHeaderUtils.checkForCSVInjectionInFileNameAndHeaders(displayName, headerFields);
        validateHeadersWithDataCloudAttr(headerFields);
        ValidateFileHeaderUtils.checkForEmptyHeaders(displayName, headerFields);
        ValidateFileHeaderUtils.checkForLongHeaders(headerFields);
        Collection<String> reservedWords = Arrays
                .asList(new String[] { ReservedField.Percentile.displayName, ReservedField.Rating.displayName });
        Collection<String> reservedBeginings = Arrays.asList(DataCloudConstants.CEAttr, DataCloudConstants.EAttr);
        ValidateFileHeaderUtils.checkForReservedHeaders(displayName, headerFields, reservedWords, reservedBeginings);
        return stream;
    }

    @Override
    public FieldMappingDocument mapRequiredFieldsWithFileHeaders(String csvFileName, String modelId) {
        ModelSummary modelSummary = modelSummaryProxy.findValidByModelId(MultiTenantContext.getTenant().getId(), modelId);
        SchemaInterpretation schemaInterpretation = getSchemaInterpretation(modelId);
        List<Attribute> requiredAttributes = modelMetadataService.getRequiredColumns(modelId);
        List<Attribute> matchingAttributes = SchemaRepository.instance()
                .getMatchingAttributes(getSchemaInterpretation(modelId));

        Set<String> scoringHeaderFields = getHeaderFields(csvFileName);

        FieldMappingDocument fieldMappingDocument = getFieldMapping(scoringHeaderFields, requiredAttributes,
                matchingAttributes);

        fieldMappingDocument.getRequiredFields().add(InterfaceName.Id.name());
        if (!modelSummary.getModelSummaryConfiguration().getBoolean(ProvenancePropertyName.ExcludePropdataColumns)
                && !plsFeatureFlagService.isFuzzyMatchEnabled()) {
            SchemaInterpretationFunctionalInterface function = (interfaceName) -> fieldMappingDocument
                    .getRequiredFields().add(interfaceName.name());
            schemaInterpretation.apply(function);
        }

        return fieldMappingDocument;
    }

    @VisibleForTesting
    FieldMappingDocument getFieldMapping(Set<String> scoringHeaderFields, List<Attribute> requiredAttributes,
            List<Attribute> matchingAttributes) {
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setFieldMappings(new ArrayList<FieldMapping>());
        fieldMappingDocument.setIgnoredFields(new ArrayList<String>());

        Iterator<Attribute> matchingAttrIter = matchingAttributes.iterator();
        while (matchingAttrIter.hasNext()) {
            Attribute matchingAttr = matchingAttrIter.next();
            requiredAttributes.stream().filter(requiredAttr -> requiredAttr.getName().equals(matchingAttr.getName()))
                    .forEach(requiredAttr -> {
                        AttributeUtils.copyPropertiesFromAttribute(matchingAttr, requiredAttr);
                        matchingAttrIter.remove();
                    });
        }
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
        ModelSummary modelSummary = modelSummaryProxy.findValidByModelId(MultiTenantContext.getTenant().getId(), modelId);
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
                .getMatchingAttributes(getSchemaInterpretation(modelId));
        matchingAttributes
                .removeIf(matchingAttr -> fieldMappingDocument.getIgnoredFields().contains(matchingAttr.getName())
                        || requiredAttributes.stream()
                                .anyMatch(requiredAttr -> requiredAttr.getName().equals(matchingAttr.getName())));

        List<Attribute> modelAttributes = new ArrayList<>(matchingAttributes);
        modelAttributes.addAll(requiredAttributes);

        SourceFile sourceFile = sourceFileService.findByName(csvFileName);
        resolveModelAttributeBasedOnFieldMapping(modelAttributes, fieldMappingDocument);

        Table table = createTableFromMetadata(modelAttributes, sourceFile);
        MetadataResolver resolver = new MetadataResolver(sourceFile.getPath(), yarnConfiguration, null);
        resolver.sortAttributesBasedOnSourceFileSequence(table);

        ModelSummary modelSummary = modelSummaryProxy.findValidByModelId(MultiTenantContext.getShortTenantId(), modelId);
        if (plsFeatureFlagService.isFuzzyMatchEnabled()) {
            SchemaInterpretationFunctionalInterface function = (interfaceName) -> {
                Attribute domainAttribute = table.getAttribute(interfaceName);
                if (domainAttribute != null) {
                    domainAttribute.setNullable(Boolean.TRUE);
                }
            };
            SchemaInterpretation.valueOf(modelSummary.getSourceSchemaInterpretation()).apply(function);
        }

        if (sourceFile.getTableName() != null) {
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            metadataProxy.deleteTable(customerSpace, sourceFile.getTableName());
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
                            "Potential conflict between scoring header %s and model attribute displayname %s",
                            sb.toString(), modelAttribute.getDisplayName()));
                    sb.setLength(len);
                    sb.append(String.format("_%d", ++version));
                }
            }
            modelAttributes.add(
                    getAttributeFromFieldName(unmappedScoringHeader, sb.toString(), fieldMapping.getMappedField()));
        }

        modelAttributes.stream().filter(attr -> fieldMappingDocument.getIgnoredFields().contains(attr.getDisplayName()))
                .forEach(attr -> {
                    List<String> approvedUsages = new ArrayList<>(attr.getApprovedUsage());
                    approvedUsages.add(ApprovedUsage.IGNORED.toString());
                    attr.setApprovedUsage(approvedUsages);
                });

        log.info(String.format("After resolving attributes, the model attributes are: %s", modelAttributes));
    }

    private Attribute getAttributeFromFieldName(String initialCSVName, String scoringHeaderName, String fieldName) {
        Attribute attribute = new Attribute();

        attribute.setName(fieldName == null
                ? ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(scoringHeaderName) : fieldName);
        attribute.setPhysicalDataType(FieldType.STRING.toString().toLowerCase());
        attribute.setDisplayName(scoringHeaderName);
        // need to populate possible csv name after renaming initial CSV name.
        // then mapper will retrieve it
        if (!initialCSVName.equals(scoringHeaderName)) {
            attribute.setPossibleCSVNames(Collections.singletonList(initialCSVName));
        }
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
                } else if (allowedDisplayNames.get(i).equalsIgnoreCase(scoringField.replace(" ", "_"))) {
                    return true;
                } else if (allowedDisplayNames.get(i).equalsIgnoreCase(scoringField.replace(" ", ""))) {
                    return true;
                }
            }
        }

        if (modelAttribute.getDisplayName() != null && (modelAttribute.getDisplayName().equalsIgnoreCase(scoringField))
                || modelAttribute.getName().equalsIgnoreCase(scoringField)) {
            return true;
        }
        return false;
    }

    @Override
    public void validateHeadersWithDataCloudAttr(Set<String> headerFields) {
        Tenant tenant = MultiTenantContext.getTenant();
        boolean considerInternalAttributes = batonService.isEnabled(MultiTenantContext.getCustomerSpace(),
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);
        List<LeadEnrichmentAttribute> attributes = attributeService.getAttributes(tenant, null, null, null,
                Boolean.TRUE, null, null, considerInternalAttributes);
        log.info("enable considerInternalAttributes is " + considerInternalAttributes);
        for (String header : headerFields) {
            for (LeadEnrichmentAttribute attribute : attributes) {
                log.info("user selected DataCloud Attribute is :" + attribute.getDisplayName());
                if (header.toLowerCase().equals(attribute.getFieldName().toLowerCase())) {
                    throw new LedpException(LedpCode.LEDP_18109,
                            new String[] { header + " conflicts with DataCloud attribute." });
                }
            }
        }
    }
}
