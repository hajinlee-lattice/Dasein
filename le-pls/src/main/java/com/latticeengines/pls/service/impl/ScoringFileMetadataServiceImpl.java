package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("scoringFileMetadataService")
public class ScoringFileMetadataServiceImpl implements ScoringFileMetadataService {

    private static final Log log = LogFactory.getLog(ScoringFileMetadataServiceImpl.class);

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

    @Override
    public InputStream validateHeaderFields(InputStream stream, List<Attribute> requiredFields,
            CloseableResourcePool closeableResourcePool, String displayName) {
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(ValidateFileHeaderUtils.BIT_PER_BYTE * ValidateFileHeaderUtils.BYTE_NUM);
        Set<String> headerFields = ValidateFileHeaderUtils.getCSVHeaderFields(stream,
                closeableResourcePool);
        try {
            stream.reset();
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        ValidateFileHeaderUtils.checkForHeaderFormat(headerFields);
        ValidateFileHeaderUtils.checkForDuplicateHeaders(requiredFields, displayName, headerFields);
        ValidateFileHeaderUtils.checkForMissingRequiredFields(requiredFields, displayName,
                headerFields, false);

        return stream;
    }

    @Override
    public Table registerMetadataTable(SourceFile sourceFile, String modelId) {
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
        if (modelSummary == null) {
            throw new RuntimeException(String.format("No such model summary with id %s", modelId));
        }
        String schemaInterpretationStr = modelSummary.getSourceSchemaInterpretation();
        if (schemaInterpretationStr == null) {
            throw new LedpException(LedpCode.LEDP_18087, new String[] { schemaInterpretationStr });
        }
        SchemaInterpretation schemaInterpretation = SchemaInterpretation
                .valueOf(schemaInterpretationStr);

        final Table table = modelMetadataService.getEventTableFromModelId(modelId);

        log.info("table is: " + table.toString());
        final Table schema = SchemaRepository.instance().getSchema(schemaInterpretation);
        Iterables.removeIf(table.getAttributes(), new Predicate<Attribute>() {
            @Override
            public boolean apply(@Nullable Attribute attr) {
                List<String> approvedUsages = attr.getApprovedUsage();
                List<String> tags = attr.getTags();
                if (schema.getAttribute(attr.getName()) == null
                        && (approvedUsages == null || approvedUsages.isEmpty()
                                || approvedUsages.get(0).equals(ApprovedUsage.NONE.toString())) //
                        || (tags == null || tags.isEmpty()
                                || !tags.get(0).equals(Tag.INTERNAL.toString()))) {
                    log.info(String.format("Removing attribute %s in table %s", attr.getName(),
                            table.getName()));
                    return true;
                }
                return false;
            }
        });

        MetadataResolver resolver = new MetadataResolver(sourceFile.getPath(), schemaInterpretation,
                yarnConfiguration, null);
        List<FieldMapping> fieldMappings = resolver.calculateBasedOnExistingMetadata(table);
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setFieldMappings(fieldMappings);
        resolver = new MetadataResolver(sourceFile.getPath(), yarnConfiguration,
                fieldMappingDocument);
        resolver.calculateBasedOnFieldMappingDocumentAndTable(table);
        log.info("After resolving table is: " + table.toString());

        // Don't dedup on primary key for scoring
        table.setPrimaryKey(null);
        table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
        table.setDisplayName(sourceFile.getDisplayName());
        Tenant tenant = MultiTenantContext.getTenant();
        metadataProxy.createTable(tenant.getId(), table.getName(), table);
        return table;
    }

    @Override
    public FieldMappingDocument mapRequiredFieldsWithFileHeaders(String csvFileName,
            String modelId) {
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
        if (modelSummary == null) {
            throw new RuntimeException(String.format("No such model summary with id %s", modelId));
        }
        String schemaInterpretationStr = modelSummary.getSourceSchemaInterpretation();
        if (schemaInterpretationStr == null) {
            throw new LedpException(LedpCode.LEDP_18087, new String[] { schemaInterpretationStr });
        }
        SchemaInterpretation schemaInterpretation = SchemaInterpretation
                .valueOf(schemaInterpretationStr);

        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setFieldMappings(new ArrayList<FieldMapping>());
        fieldMappingDocument.setIgnoredFields(new ArrayList<String>());

        fieldMappingDocument.getRequiredFields().add(InterfaceName.Id.name());
        if (schemaInterpretation == SchemaInterpretation.SalesforceAccount) {
            fieldMappingDocument.getRequiredFields().add(InterfaceName.Website.name());
        } else {
            fieldMappingDocument.getRequiredFields().add(InterfaceName.Email.name());
        }

        Set<String> scoringHeaderFields = getHeaderFields(csvFileName);
        List<Attribute> requiredAttributes = modelMetadataService.getRequiredColumns(modelId);
        Iterator<Attribute> requiredAttributesIterator = requiredAttributes.iterator();
        while (requiredAttributesIterator.hasNext()) {
            Attribute requiredAttribute = requiredAttributesIterator.next();
            Iterator<String> scoringHeaderFieldsIterator = scoringHeaderFields.iterator();

            FieldMapping fieldMapping = new FieldMapping();
            while (scoringHeaderFieldsIterator.hasNext()) {
                String scoringHeaderField = scoringHeaderFieldsIterator.next();
                if (isScoringFieldMatchedWithModelAttribute(scoringHeaderField,
                        requiredAttribute)) {
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

    @Override
    public Table saveFieldMappingDocument(String csvFileName, String modelId,
            FieldMappingDocument fieldMappingDocument) {
        List<Attribute> modelAttributes = modelMetadataService.getRequiredColumns(modelId);

        SourceFile sourceFile = sourceFileService.findByName(csvFileName);
        resolveModelAttributeBasedOnFieldMapping(modelAttributes, fieldMappingDocument);
        Table table = createTableFromMetadata(modelAttributes, sourceFile);
        table.deduplicateAttributeNames(modelMetadataService.getLatticeAttributeNames(modelId));
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

    private void resolveModelAttributeBasedOnFieldMapping(List<Attribute> modelAttributes,
            FieldMappingDocument fieldMappingDocument) {
        Iterator<Attribute> attrIterator = modelAttributes.iterator();
        while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
                if (fieldMapping.isMappedToLatticeField()) {
                    if (fieldMapping.getMappedField().equals(attribute.getName())) {
                        attribute.setDisplayName(fieldMapping.getUserField());
                        break;
                    }
                }
            }
        }

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                if (fieldMapping.getUserField() == null) {
                    continue;
                }
                String unmappedScoringHeader = fieldMapping.getUserField();
                for (Attribute modelAttribute : modelAttributes) {
                    if (modelAttribute.getName().equals(unmappedScoringHeader)
                            || modelAttribute.getDisplayName().equals(unmappedScoringHeader)) {
                        unmappedScoringHeader = unmappedScoringHeader.concat("_1");
                    }
                }
                modelAttributes.add(getAttributeFromFieldName(unmappedScoringHeader));
            }
        }
    }

    private Attribute getAttributeFromFieldName(String fieldName) {
        Attribute attribute = new Attribute();

        attribute.setName(ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(fieldName));
        attribute.setPhysicalDataType(FieldType.STRING.toString().toLowerCase());
        attribute.setDisplayName(fieldName);
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

    private boolean isScoringFieldMatchedWithModelAttribute(String scoringField,
            Attribute modelAttribute) {
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
