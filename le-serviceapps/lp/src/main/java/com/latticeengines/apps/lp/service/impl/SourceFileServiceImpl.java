package com.latticeengines.apps.lp.service.impl;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.lp.entitymgr.SourceFileEntityMgr;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CopySourceFileRequest;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

@Service("sourceFileService")
public class SourceFileServiceImpl implements SourceFileService {

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
            Tenant targetTenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(request.getTargetTenant()).toString());
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
        String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(targetTenant.getId())).toString()
                + "/" + outputFileName;
        try {
            String srcFile = getS3Path(originalSourceFile.getTenant().getId(), originalSourceFile.getPath());
            if (!HdfsUtils.fileExists(yarnConfiguration, outputPath)) {
                HdfsUtils.copyFiles(yarnConfiguration, srcFile, outputPath);
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

    private String getS3Path(String customerSpace, String hdfsPath) throws IOException {
        String protocol = Boolean.TRUE.equals(useEmr) ? "s3a" : "s3n";
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(protocol);
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        String s3Path = pathBuilder.exploreS3FilePath(hdfsPath, podId, space.toString(),
                space.getTenantId(), s3Bucket);
        if (HdfsUtils.fileExists(yarnConfiguration, s3Path)) {
            hdfsPath = s3Path;
        }
        return hdfsPath;
    }
}
