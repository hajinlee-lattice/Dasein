package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.metadata.resolution.ColumnTypeMapping;
import com.latticeengines.pls.metadata.resolution.MetadataResolutionStrategy;
import com.latticeengines.pls.metadata.resolution.UserDefinedMetadataResolutionStrategy;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("fileUploadService")
public class FileUploadServiceImpl implements FileUploadService {

    private static final Log log = LogFactory.getLog(FileUploadServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public SourceFile uploadFile(String outputFileName, SchemaInterpretation schemaInterpretation,
            InputStream inputStream) {
        CSVParser parser = null;
        try {
            Tenant tenant = SecurityContextUtils.getTenant();
            tenant = tenantEntityMgr.findByTenantId(tenant.getId());
            CustomerSpace space = CustomerSpace.parse(tenant.getId());
            String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
            SourceFile file = new SourceFile();
            file.setTenant(tenant);
            file.setName(outputFileName);
            file.setPath(outputPath + "/" + outputFileName);
            file.setSchemaInterpretation(schemaInterpretation);
            file.setState(SourceFileState.Uploaded);

            if (!inputStream.markSupported()) {
                inputStream = new BufferedInputStream(inputStream);
            }
            inputStream.mark(1024 * 500);

            Set<String> headerFields = null;
            InputStreamReader reader = new InputStreamReader(new BOMInputStream(inputStream, false,
                    ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                    ByteOrderMark.UTF_32BE), StandardCharsets.UTF_8);
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');

            try {
                parser = new CSVParser(reader, format);
                headerFields = parser.getHeaderMap().keySet();
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_18094, e);
            }

            MetadataResolutionStrategy strategy = getResolutionStrategy(file, null);
            strategy.validateHeaderFields(headerFields, schemaInterpretation, file.getPath());

            inputStream.reset();
            HdfsUtils
                    .copyInputStreamToHdfsWithoutBom(yarnConfiguration, inputStream, outputPath + "/" + outputFileName);
            sourceFileService.create(file);
            return sourceFileService.findByName(file.getName());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18053, e, new String[] { outputFileName });
        } finally {
            try {
                parser.close();
            } catch (IOException e) {
                log.error(e);
                e.printStackTrace();
                throw new LedpException(LedpCode.LEDP_00002);
            }
        }
    }

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

        String customerSpace = SecurityContextUtils.getTenant().getId().toString();

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
    public InputStream getImportErrorStream(String fileName) {

        SourceFile sourceFile = sourceFileService.findByName(fileName);
        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { fileName });
        }

        try {
            if (sourceFile.getState() != SourceFileState.Imported) {
                throw new RuntimeException(String.format("File %s has not been imported yet.", fileName));
            }

            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            Table table = metadataProxy.getTable(SecurityContextUtils.getCustomerSpace().toString(),
                    sourceFile.getTableName());

            Path schemaPath = new Path(table.getExtractsDirectory() + "/error.csv");
            return fs.open(schemaPath);

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18085, new String[] { fileName });
        }
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
