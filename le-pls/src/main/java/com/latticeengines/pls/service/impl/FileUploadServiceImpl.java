package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.io.InputStream;

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
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

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
    public SourceFile uploadFile(String outputFileName, SchemaInterpretation schemaInterpretation, String displayName,
                                 InputStream inputStream) {
        log.info(String.format(
                "Uploading file (outputFileName=%s, schemaInterpretation=%s, displayName=%s, customer=%s)",
                outputFileName, schemaInterpretation, displayName, MultiTenantContext.getCustomerSpace()));
        try {
            Tenant tenant = MultiTenantContext.getTenant();
            tenant = tenantEntityMgr.findByTenantId(tenant.getId());
            CustomerSpace space = CustomerSpace.parse(tenant.getId());
            String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
            SourceFile file = new SourceFile();
            file.setTenant(tenant);
            file.setName(outputFileName);
            file.setPath(outputPath + "/" + outputFileName);
            file.setSchemaInterpretation(schemaInterpretation);
            file.setState(SourceFileState.Uploaded);
            file.setDisplayName(displayName);

            HdfsUtils
                    .copyInputStreamToHdfsWithoutBom(yarnConfiguration, inputStream, outputPath + "/" + outputFileName);
            sourceFileService.create(file);
            return sourceFileService.findByName(file.getName());
        } catch (IOException e) {
            log.error(String.format("Problems uploading file %s (display name %s)", outputFileName, displayName), e);
            throw new LedpException(LedpCode.LEDP_18053, e, new String[] { displayName });
        }
    }

    @Override
    public SourceFile uploadFile(String outputFileName, String displayName, InputStream inputStream) {
        log.info(String.format(
                "Uploading file (outputFileName=%s, displayName=%s, customer=%s)",
                outputFileName, displayName, MultiTenantContext.getCustomerSpace()));
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

            HdfsUtils
                    .copyInputStreamToHdfsWithoutBom(yarnConfiguration, inputStream, outputPath + "/" + outputFileName);
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

}
