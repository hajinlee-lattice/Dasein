package com.latticeengines.apps.lp.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.lp.entitymgr.SourceFileEntityMgr;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CopySourceFileRequest;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

@Service("sourceFileService")
public class SourceFileServiceImpl implements SourceFileService {

    private static final Logger log = LoggerFactory.getLogger(SourceFileServiceImpl.class);

    @Inject
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${pls.fileupload.maxupload.rows}")
    private long maxUploadRows;

    @Inject
    private S3Service s3Service;

    @Override
    public SourceFile getByTableNameCrossTenant(String tableName) {
        return sourceFileEntityMgr.getByTableName(tableName);
    }

    @Override
    public SourceFile findByName(String name) {
        return sourceFileEntityMgr.findByName(name);
    }

    @Override
    public SourceFile findByTableName(String tableName) {
        return sourceFileEntityMgr.findByTableName(tableName);
    }

    @Override
    public SourceFile findByApplicationId(String applicationId) {
        return sourceFileEntityMgr.findByApplicationId(applicationId);
    }

    @Override
    public void create(SourceFile sourceFile) {
        Tenant tenant = MultiTenantContext.getTenant();
        sourceFile.setTenant(tenant);
        sourceFileEntityMgr.create(sourceFile);
    }

    @Override
    public void update(SourceFile sourceFile) {
        SourceFile existing = sourceFileEntityMgr.findByName(sourceFile.getName());
        if (existing != null) {
            sourceFileEntityMgr.delete(existing);
        }
        create(sourceFile);
    }

    @Override
    public void delete(String name) {
        SourceFile sourceFile = sourceFileEntityMgr.findByName(name);
        if (sourceFile != null) {
            sourceFileEntityMgr.delete(sourceFile);
        }
    }

    @Override
    public void copySourceFile(CopySourceFileRequest request) {
        SourceFile originalSourceFile = findByName(request.getOriginalSourceFile());
        if (originalSourceFile != null) {
            Tenant targetTenant = tenantEntityMgr
                    .findByTenantId(CustomerSpace.parse(request.getTargetTenant()).toString());
            if (targetTenant != null) {
                copySourceFile(request.getTargetTable(), originalSourceFile, targetTenant);
            }
        }
    }

    @Override
    public List<SourceFile> findAllSourceFiles() {
        return sourceFileEntityMgr.findAllSourceFiles();
    }

    private void copySourceFile(String tableName, SourceFile originalSourceFile, Tenant targetTenant) {
        String outputFileName = "file_" + tableName + ".csv";
        String outputPath = PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(targetTenant.getId())).toString()
                + "/" + outputFileName;
        try {
            String srcFile = new HdfsToS3PathBuilder(useEmr).getS3PathWithGlob(yarnConfiguration,
                    originalSourceFile.getPath(), false, s3Bucket);
            if (!HdfsUtils.fileExists(yarnConfiguration, outputPath)) {
                log.info(String.format("Copying source file from %s to %s", srcFile, outputPath));
                HdfsUtils.copyFiles(yarnConfiguration, srcFile, outputPath);
                String s3OutputPath = new HdfsToS3PathBuilder(useEmr).exploreS3FilePath(outputPath, s3Bucket);
                log.info(String.format("Copying source file from %s to %s", srcFile, s3OutputPath));
                HdfsUtils.copyFiles(yarnConfiguration, srcFile, s3OutputPath);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18118, e);
        }
        SourceFile file = new SourceFile();
        file.setApplicationId(originalSourceFile.getApplicationId());
        file.setDisplayName(originalSourceFile.getDisplayName());
        file.setName(outputFileName);
        file.setPath(outputPath);
        file.setSchemaInterpretation(originalSourceFile.getSchemaInterpretation());
        file.setState(originalSourceFile.getState());
        file.setTableName(tableName);
        sourceFileEntityMgr.create(file, targetTenant);
    }

    @Override
    public SourceFile createSourceFileFromS3(String customerSpace, FileProperty fileProperty,
                                             SchemaInterpretation schemaInterpretation,
                                             String entity) {
        String s3FilePath = fileProperty.getFilePath();
        String key = formatString(s3FilePath);
        if (key.startsWith(s3Bucket)) {
            key = key.replaceFirst(s3Bucket, "");
            key = formatString(key);
        }
        InputStream inputStream = s3Service.readObjectAsStream(s3Bucket, key);
        String outputHdfsPath = null;
        String outputFileName = fileProperty.getFileName();
        try {
            CustomerSpace space = CustomerSpace.parse(customerSpace);
            String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
            outputHdfsPath = outputPath;
            SourceFile file = new SourceFile();
            file.setName(outputFileName);
            file.setPath(outputPath + "/" + outputFileName);
            file.setSchemaInterpretation(schemaInterpretation);
            file.setBusinessEntity(StringUtils.isEmpty(entity) ? null : BusinessEntity.getByName(entity));
            file.setState(SourceFileState.Uploaded);
            file.setDisplayName(outputFileName);

            long fileRows = HdfsUtils.copyInputStreamToHdfsWithoutBomAndReturnRows(yarnConfiguration, inputStream,
                    outputPath + "/" + outputFileName, maxUploadRows);
            log.info(String.format("current file outputFileName=%s fileRows = %s", outputFileName, fileRows));
            file.setFileRows(fileRows);
            sourceFileEntityMgr.create(file);
            return sourceFileEntityMgr.findByName(file.getName());
        } catch (IOException e) {
            if (outputHdfsPath != null) {
                try {
                    HdfsUtils.rmdir(yarnConfiguration, outputHdfsPath + "/" + outputFileName);
                } catch (IOException e1) {
                    log.error(String.format("error when deleting file %s in hdfs",
                            outputHdfsPath + "/" + outputFileName));
                }
            }
            log.error(String.format("Problems uploading file %s (display name %s)", outputFileName, outputFileName), e);
            throw new LedpException(LedpCode.LEDP_18053, e, new String[] { outputFileName });
        }
    }

    private String formatString(String path) {
        if (StringUtils.isNotEmpty(path)) {
            while (path.startsWith("/")) {
                path = path.substring(1);
            }
            while (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
        }
        return path;
    }

}
