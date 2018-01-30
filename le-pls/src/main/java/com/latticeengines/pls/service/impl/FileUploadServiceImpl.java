package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("fileUploadService")
public class FileUploadServiceImpl implements FileUploadService {

    private static final Logger log = LoggerFactory.getLogger(FileUploadServiceImpl.class);

    private static final int MAX_RETRY = 3;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private CDLProxy cdlProxy;

    @Value("${pls.fileupload.maxupload.rows}")
    private long maxUploadRows;

    @Override
    public SourceFile uploadFile(String outputFileName, //
            SchemaInterpretation schemaInterpretation, //
            String entity, //
            String displayName, //
            InputStream inputStream) {
        log.info(String.format(
                "Uploading file (outputFileName=%s, schemaInterpretation=%s, displayName=%s, customer=%s)",
                outputFileName, schemaInterpretation, displayName, MultiTenantContext.getCustomerSpace()));
        String outputHdfsPath = null;
        try {
            Tenant tenant = MultiTenantContext.getTenant();
            tenant = tenantEntityMgr.findByTenantId(tenant.getId());
            CustomerSpace space = CustomerSpace.parse(tenant.getId());
            String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
            outputHdfsPath = outputPath;
            SourceFile file = new SourceFile();
            file.setTenant(tenant);
            file.setName(outputFileName);
            file.setPath(outputPath + "/" + outputFileName);
            file.setSchemaInterpretation(schemaInterpretation);
            file.setBusinessEntity(StringUtils.isEmpty(entity) ? null : BusinessEntity.getByName(entity));
            file.setState(SourceFileState.Uploaded);
            file.setDisplayName(displayName);

            long fileRows = HdfsUtils.copyInputStreamToHdfsWithoutBomAndReturnRows(yarnConfiguration, inputStream,
                    outputPath + "/" + outputFileName, maxUploadRows);
            log.info(String.format("current file outputFileName=%s fileRows = %s", outputFileName, fileRows));
            if (fileRows > maxUploadRows) {
                try {
                    HdfsUtils.rmdir(yarnConfiguration, outputPath + "/" + outputFileName);
                } catch (Exception e) {
                    log.error(String.format("error when deleting file %s in hdfs", outputPath + "/" + outputFileName));
                }
                double rows = (double) fileRows / 1000000;
                throw new LedpException(LedpCode.LEDP_18148, new String[] { String.format("%.2f", rows) });
            }
            sourceFileService.create(file);
            return getSourceFilewithRetry(file.getName());
        } catch (IOException e) {
            if (outputHdfsPath != null) {
                try {
                    HdfsUtils.rmdir(yarnConfiguration, outputHdfsPath + "/" + outputFileName);
                } catch (IOException e1) {
                    log.error(String.format("error when deleting file %s in hdfs",
                            outputHdfsPath + "/" + outputFileName));
                }
            }
            log.error(String.format("Problems uploading file %s (display name %s)", outputFileName, displayName), e);
            throw new LedpException(LedpCode.LEDP_18053, e, new String[] { displayName });
        }
    }

    private SourceFile getSourceFilewithRetry(String fileName) {
        int retry = 1;
        SourceFile sourceFile = sourceFileService.findByName(fileName);
        while (sourceFile == null) {
            if (retry > MAX_RETRY) {
                throw new LedpException(LedpCode.LEDP_18053, new String[] { fileName });
            }
            log.error(String.format("Cannot find source file with name: %s, retry count: %d", fileName, retry));
            try {
                Thread.sleep(500);
                retry++;
                sourceFile = sourceFileService.findByName(fileName);
            } catch (InterruptedException e) {
                log.info("Sleep thread interrupted.");
            }
        }
        return sourceFile;
    }

    @Override
    public SourceFile uploadFile(String outputFileName, String displayName, InputStream inputStream) {
        log.info(String.format("Uploading file (outputFileName=%s, displayName=%s, customer=%s)", outputFileName,
                displayName, MultiTenantContext.getCustomerSpace()));
        try {
            Tenant tenant = MultiTenantContext.getTenant();
            tenant = tenantEntityMgr.findByTenantId(tenant.getId());
            CustomerSpace space = CustomerSpace.parse(tenant.getId());
            String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
            SourceFile file = new SourceFile();
            file.setTenant(tenant);
            file.setName(outputFileName);
            file.setPath(outputPath + "/" + outputFileName);
            file.setState(SourceFileState.Uploaded);
            file.setDisplayName(displayName);

            HdfsUtils.copyInputStreamToHdfsWithoutBom(yarnConfiguration, inputStream,
                    outputPath + "/" + outputFileName);
            sourceFileService.create(file);
            return sourceFileService.findByName(file.getName());
        } catch (IOException e) {
            log.error(String.format("Problems uploading file %s (display name %s)", outputFileName, displayName), e);
            throw new LedpException(LedpCode.LEDP_18053, e, new String[] { displayName });
        }
    }

    @Override
    public Table getMetadata(String fileName) {
        SourceFile sourceFile = sourceFileService.findByName(fileName);
        if (sourceFile == null) {
            return null;
        }
        if (sourceFile.getTableName() == null) {
            return null;
        }
        return metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), sourceFile.getTableName());
    }

    @Override
    public InputStream getImportErrorStream(String fileName) {
        SourceFile sourceFile = sourceFileService.findByName(fileName);
        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { fileName });
        }

        try {
            if (sourceFile.getState() != SourceFileState.Imported) {
                throw new LedpException(LedpCode.LEDP_18101, new String[] { sourceFile.getDisplayName() });
            }

            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            Table table = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                    sourceFile.getTableName());

            Path schemaPath = new Path(table.getExtractsDirectory() + "/error.csv");
            return fs.open(schemaPath);

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18085, e, new String[] { sourceFile.getDisplayName() });
        }
    }

    @Override
    public ResponseDocument<String> cleanupByUpload(SourceFile sourceFile, SchemaInterpretation schemaInterpretation,
            BusinessEntity entity, CleanupOperationType cleanupOperationType) {
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), schemaInterpretation, null);

        List<FieldMapping> fieldMappings = new ArrayList<>();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (!StringUtils.isEmpty(fieldMapping.getMappedField())) {
                fieldMappings.add(fieldMapping);
            }
        }
        fieldMappingDocument.setFieldMappings(fieldMappings);
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument);

        sourceFile = sourceFileService.findByName(sourceFile.getName());
        String email = MultiTenantContext.getEmailAddress();
        log.info(String.format("table name is: %s, file path:  %s, email: %s", sourceFile.getTableName(),
                sourceFile.getPath(), email));
        return cdlProxy.cleanupByUpload(MultiTenantContext.getCustomerSpace().toString(), sourceFile, entity,
                cleanupOperationType, email);

    }
}
