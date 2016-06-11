package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.metadata.resolution.ColumnTypeMapping;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
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

    @Override
    public InputStream validateHeaderFields(InputStream stream, List<Attribute> requiredFields,
            CloseableResourcePool closeableResourcePool, String displayName) {
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(ValidateFileHeaderUtils.BIT_PER_BYTE * ValidateFileHeaderUtils.BYTE_NUM);
        Set<String> headerFields = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        try {
            stream.reset();
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        ValidateFileHeaderUtils.checkForHeaderFormat(headerFields);
        ValidateFileHeaderUtils.checkForDuplicateHeaders(requiredFields, displayName, headerFields);
        ValidateFileHeaderUtils.checkForMissingRequiredFields(requiredFields, displayName, headerFields, false);

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
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.valueOf(schemaInterpretationStr);

        Table table = modelMetadataService.getEventTableFromModelId(modelId);
        MetadataResolver resolver = new MetadataResolver(sourceFile.getPath(), schemaInterpretation, null,
                yarnConfiguration);
        resolver.calculateBasedOnExistingMetadata(table);
        if (!resolver.isMetadataFullyDefined()) {
            List<ColumnTypeMapping> unknown = resolver.getUnknownColumns();
            resolver = new MetadataResolver(sourceFile.getPath(), schemaInterpretation, unknown, yarnConfiguration);
            resolver.calculateBasedOnExistingMetadata(table);
        }

        Iterables.removeIf(table.getAttributes(), new Predicate<Attribute>() {
            @Override
            public boolean apply(@Nullable Attribute attr) {
                List<String> approvedUsages = attr.getApprovedUsage();
                List<String> tags = attr.getTags();
                return (approvedUsages == null || approvedUsages.isEmpty()
                                || approvedUsages.get(0).equals(ApprovedUsage.NONE.toString())) //
                        || (tags == null || tags.isEmpty() || !tags.get(0).equals(Tag.INTERNAL.toString()));
            }
        });

        // Don't dedup on primary key for scoring
        table.setPrimaryKey(null);

        table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
        Tenant tenant = MultiTenantContext.getTenant();
        metadataProxy.createTable(tenant.getId(), table.getName(), table);
        return table;
    }
}
