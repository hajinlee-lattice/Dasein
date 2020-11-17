package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("fileUploadService")
public class FileUploadServiceImpl implements FileUploadService {

    private static final Logger log = LoggerFactory.getLogger(FileUploadServiceImpl.class);

    private static final int MAX_RETRY = 3;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private ModelingFileMetadataService modelingFileMetadataService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private MetadataProxy metadataProxy;

    @Value("${pls.fileupload.maxupload.rows}")
    private long maxUploadRows;

    @Value("${pls.fileupload.maxupload.bytes}")
    private long maxUploadSize;

    @Override
    public SourceFile uploadFile(String outputFileName, //
            SchemaInterpretation schemaInterpretation, //
            String entity, //
            String displayName, //
            InputStream inputStream) {
        return uploadFile(outputFileName, schemaInterpretation, entity, displayName, inputStream, false);
    }

    @Override
    public SourceFile uploadFile(String outputFileName, //
            SchemaInterpretation schemaInterpretation, //
            String entity, //
            String displayName, //
            InputStream inputStream,
            boolean outsizeFlag) {
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

            long fileRows = saveFileToHdfs(outputFileName, inputStream, outputPath, !outsizeFlag);
            file.setFileRows(fileRows);
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

    @Override
    public SourceFile uploadFile(String outputFileName, String displayName, EntityType entityType, InputStream inputStream) {
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
            file.setSchemaInterpretation(entityType == null ? null : entityType.getSchemaInterpretation());
            file.setBusinessEntity(entityType == null ? null : entityType.getEntity());
            file.setState(SourceFileState.Uploaded);
            file.setDisplayName(displayName);

            long fileRows = saveFileToHdfs(outputFileName, inputStream, outputPath, false);
            file.setFileRows(fileRows);
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

    private long saveFileToHdfs(String outputFileName, InputStream inputStream, String outputPath, boolean checkSize) throws IOException {
        long fileRows = HdfsUtils.copyInputStreamToHdfsWithoutBomAndReturnRows(yarnConfiguration, inputStream,
                outputPath + "/" + outputFileName, maxUploadRows);

        log.info(String.format("current file outputFileName=%s fileRows = %s", outputFileName, fileRows));
        if (checkSize && fileRows > maxUploadRows) {
            try {
                HdfsUtils.rmdir(yarnConfiguration, outputPath + "/" + outputFileName);
            } catch (Exception e) {
                log.error(String.format("error when deleting file %s in hdfs", outputPath + "/" + outputFileName));
            }
            double rows = (double) fileRows / 1000000;
            throw new LedpException(LedpCode.LEDP_18148, new String[] { String.format("%.2f", rows) });
        }
        return fileRows;
    }

    private SourceFile getSourceFilewithRetry(String fileName, boolean withTable) {
        int retry = 1;
        SourceFile sourceFile = sourceFileService.findByName(fileName);
        while (sourceFile == null || (withTable && StringUtils.isEmpty(sourceFile.getTableName()))) {
            if (retry > MAX_RETRY) {
                throw new LedpException(LedpCode.LEDP_18053, new String[] { fileName });
            }
            log.error(String.format("Cannot find source file with name: %s or table name is empty, retry count: %d",
                    fileName, retry));
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

    private SourceFile getSourceFilewithRetry(String fileName) {
        return getSourceFilewithRetry(fileName, false);
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
    public SourceFile uploadCleanupFileTemplate(SourceFile sourceFile, SchemaInterpretation schemaInterpretation,
            CleanupOperationType cleanupOperationType, boolean enableEntityMatch, boolean onlyGA) {
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), schemaInterpretation, null, false, false,
                        enableEntityMatch, onlyGA);

        List<FieldMapping> fieldMappings = new ArrayList<>();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (!StringUtils.isEmpty(fieldMapping.getMappedField())) {
                fieldMappings.add(fieldMapping);
            }
        }
        fieldMappingDocument.setFieldMappings(fieldMappings);
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, false,
                enableEntityMatch, onlyGA);

        sourceFile = getSourceFilewithRetry(sourceFile.getName(), true);
        Table template = getMetadata(sourceFile.getName());
        if (template == null) {
            Table standard = SchemaRepository.instance().getSchema(schemaInterpretation, false, enableEntityMatch,
                    onlyGA);
            Preconditions.checkNotNull(standard, "standard table should not be null");
            Set<String> requiredColumns =
                    standard.getAttributes().stream()
                            .filter(Attribute::getRequired)
                            .map(Attribute::getName)
                            .collect(Collectors.toSet());
            throw new RuntimeException(String.format("Cannot resolve metadata from uploaded file, please provide " +
                    "required columns %s!", requiredColumns));
        }
        InterfaceName accountInterface = enableEntityMatch ? InterfaceName.CustomerAccountId : InterfaceName.AccountId;
        InterfaceName contactInterface = enableEntityMatch ? InterfaceName.CustomerContactId : InterfaceName.ContactId;

        switch (schemaInterpretation) {
            case RegisterDeleteDataTemplate:
                if (template.getAttribute(InterfaceName.AccountId) == null) {
                    throw new LedpException(LedpCode.LEDP_40073,
                            new String[] { InterfaceName.AccountId.name() });
                }
                break;
            case DeleteAccountTemplate:
                if (template.getAttribute(accountInterface) == null) {
                    throw new LedpException(LedpCode.LEDP_40007,
                            new String[] { "Account", accountInterface.name() });
                }
                break;
            case DeleteContactTemplate:
                if (template.getAttribute(contactInterface) == null) {
                    throw new LedpException(LedpCode.LEDP_40007,
                            new String[] { "Contact", contactInterface.name() });
                }
                break;
            case DeleteTransactionTemplate:
                switch (cleanupOperationType) {
                    case BYUPLOAD_ACPD:
                        if (template.getAttribute(accountInterface) == null) {
                            throw new LedpException(LedpCode.LEDP_40007,
                                    new String[] { "Delete by ACPD", accountInterface.name() });
                        }
                        if (template.getAttribute(contactInterface) == null) {
                            throw new LedpException(LedpCode.LEDP_40007,
                                    new String[] { "Delete by ACPD", contactInterface.name() });
                        }
                        if (template.getAttribute(InterfaceName.ProductId) == null) {
                            throw new LedpException(LedpCode.LEDP_40007,
                                    new String[] { "Delete by ACPD", InterfaceName.ProductId.name() });
                        }
                        if (template.getAttribute(InterfaceName.TransactionTime) == null) {
                            throw new LedpException(LedpCode.LEDP_40007,
                                    new String[] { "Delete by ACPD", InterfaceName.TransactionTime.name() });
                        }
                        break;
                    case BYUPLOAD_MINDATE:
                        if (template.getAttribute(InterfaceName.TransactionTime) == null) {
                            throw new LedpException(LedpCode.LEDP_40007,
                                    new String[] { "Delete by MIN date", InterfaceName.TransactionTime.name() });
                        }
                        break;
                    case BYUPLOAD_MINDATEANDACCOUNT:
                        if (template.getAttribute(accountInterface) == null) {
                            throw new LedpException(LedpCode.LEDP_40007,
                                    new String[] { "Delete by MIN date & Account", accountInterface.name() });
                        }
                        if (template.getAttribute(InterfaceName.TransactionTime) == null) {
                            throw new LedpException(LedpCode.LEDP_40007,
                                    new String[] { "Delete by MIN date & Account", InterfaceName.TransactionTime.name() });
                        }
                        break;
                    default:
                    }
                break;
            default:
        }
        return sourceFile;

    }

    @Override
    public SourceFile createSourceFileFromS3(FileProperty fileProperty, String entity) {
        return sourceFileService.createSourceFileFromS3(fileProperty, entity);
    }

    @Override
    public SourceFileInfo uploadFile(String name, String displayName, boolean compressed, EntityType entityType, MultipartFile file) {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            log.info(String.format("Uploading file %s (displayName=%s, compressed=%s)", name, displayName, compressed));

            if (file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();

            // decompress
            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }

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

            ValidateFileHeaderUtils.checkForDuplicatedHeaders(headerFields);
            // save file
            SourceFile sourceFile = uploadFile(name, displayName, entityType, stream);
            return getSourceFileInfo(sourceFile);
        } catch (IOException e) {
            log.error("Cannot get input stream for file: " + name);
            throw new LedpException(LedpCode.LEDP_18053, new String[] { displayName });
        }
    }

    private SourceFileInfo getSourceFileInfo(SourceFile sourceFile) {
        if (sourceFile == null) {
            return null;
        }
        SourceFileInfo sourceFileInfo = new SourceFileInfo();
        sourceFileInfo.setFileImportId(sourceFile.getName());
        sourceFileInfo.setDisplayName(sourceFile.getDisplayName());
        sourceFileInfo.setFileRows(sourceFile.getFileRows());
        return sourceFileInfo;
    }
}
