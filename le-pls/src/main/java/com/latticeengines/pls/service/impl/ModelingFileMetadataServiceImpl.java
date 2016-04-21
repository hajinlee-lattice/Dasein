package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.metadata.resolution.ColumnTypeMapping;
import com.latticeengines.pls.metadata.resolution.MetadataResolutionStrategy;
import com.latticeengines.pls.metadata.resolution.UserDefinedMetadataResolutionStrategy;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("modelingFileMetadataService")
public class ModelingFileMetadataServiceImpl implements ModelingFileMetadataService {
    private static final Logger log = Logger.getLogger(ModelingFileMetadataServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public List<ColumnTypeMapping> getUnknownColumns(String sourceFileName) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        MetadataResolutionStrategy strategy = getResolutionStrategy(sourceFile, null);
        strategy.calculate();
        return strategy.getUnknownColumns();
    }

    @Override
    public void resolveMetadata(String sourceFileName, List<ColumnTypeMapping> additionalColumns) {
        SourceFile sourceFile = getSourceFile(sourceFileName);
        MetadataResolutionStrategy strategy = getResolutionStrategy(sourceFile, additionalColumns);
        strategy.calculate();
        if (!strategy.isMetadataFullyDefined()) {
            throw new RuntimeException(String.format("Metadata is not fully defined for file %s", sourceFileName));
        }

        String customerSpace = MultiTenantContext.getTenant().getId().toString();

        if (sourceFile.getTableName() != null) {
            metadataProxy.deleteTable(customerSpace, sourceFile.getTableName());
        }

        Table table = strategy.getMetadata();
        table.setName("SourceFile_" + sourceFileName.replace(".", "_"));
        metadataProxy.createTable(customerSpace, table.getName(), table);
        sourceFile.setTableName(table.getName());
        sourceFileService.update(sourceFile);
    }

    @Override
    public InputStream validateHeaderFields(InputStream stream, SchemaInterpretation schema,
            CloseableResourcePool closeableResourcePool, String fileDisplayName) {

        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }

        stream.mark(1024 * 500);

        Set<String> headerFields = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        try {
            stream.reset();
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }

        SchemaRepository repository = SchemaRepository.instance();
        Table metadata = repository.getSchema(schema);

        Set<String> missingRequiredFields = new HashSet<>();
        List<Attribute> attributes = metadata.getAttributes();
        Iterator<Attribute> attrIterator = attributes.iterator();

        iterateAttr: while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            Iterator<String> headerIterator = headerFields.iterator();

            while (headerIterator.hasNext()) {
                String header = headerIterator.next();
                if (attribute.getAllowedDisplayNames().contains(header)) {
                    attrIterator.remove();
                    headerIterator.remove();
                    continue iterateAttr;
                }
            }
            if (!attribute.isNullable()) {
                missingRequiredFields.add(attribute.getName());
            }
        }
        if (!missingRequiredFields.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_18087, //
                    new String[] { StringUtils.join(missingRequiredFields, ","), fileDisplayName });
        }

        for (final String field : headerFields) {
            if (StringUtils.isEmpty(field)) {
                throw new LedpException(LedpCode.LEDP_18096, new String[] { fileDisplayName });
            }
        }

        return stream;

    }

    private SourceFile getSourceFile(String sourceFileName) {
        SourceFile sourceFile = sourceFileService.findByName(sourceFileName);
        if (sourceFile == null) {
            throw new RuntimeException(String.format("Could not locate source file with name %s", sourceFileName));
        }
        return sourceFile;
    }

    private MetadataResolutionStrategy getResolutionStrategy(SourceFile sourceFile,
            List<ColumnTypeMapping> additionalColumns) {
        return new UserDefinedMetadataResolutionStrategy(sourceFile.getPath(), //
                sourceFile.getSchemaInterpretation(), additionalColumns, yarnConfiguration);
    }
}
