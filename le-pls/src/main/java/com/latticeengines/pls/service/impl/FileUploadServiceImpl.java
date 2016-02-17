package com.latticeengines.pls.service.impl;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.domain.exposed.workflow.SourceFileState;
import com.latticeengines.metadata.exposed.resolution.ColumnTypeMapping;
import com.latticeengines.metadata.exposed.resolution.MetadataResolutionStrategy;
import com.latticeengines.metadata.exposed.resolution.UserDefinedMetadataResolutionStrategy;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.latticeengines.workflow.exposed.service.SourceFileService;

@Component("fileUploadService")
public class FileUploadServiceImpl implements FileUploadService {

    @SuppressWarnings("unused")
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
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, inputStream, outputPath + "/" + outputFileName);
            sourceFileService.create(file);
            return sourceFileService.findByName(file.getName());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18053, e);
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
            metadataProxy.deleteImportTable(customerSpace, sourceFile.getTableName());
        }

        Table table = strategy.getMetadata();
        table.setName("SourceFile_" + sourceFileName.replace(".", "_"));
        metadataProxy.createTable(customerSpace, table.getName(), table);
        sourceFile.setTableName(table.getName());
        sourceFileService.update(sourceFile);
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
