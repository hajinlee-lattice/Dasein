package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;

@Component("sourceFileService")
public class SourceFileServiceImpl implements SourceFileService {

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Override
    public SourceFile findByName(String name) {
        return sourceFileProxy.findByName(getShortTenantId(), name);
    }

    @Override
    public SourceFile findByApplicationId(String applicationId) {
        return sourceFileProxy.findByApplicationId(getShortTenantId(), applicationId);
    }

    @Override
    public void create(SourceFile sourceFile) {
        sourceFileProxy.create(getShortTenantId(), sourceFile);
    }

    @Override
    public void update(SourceFile sourceFile) {
        sourceFileProxy.update(getShortTenantId(), sourceFile);
    }

    @Override
    public void delete(SourceFile sourceFile) {
        sourceFileProxy.delete(getShortTenantId(), sourceFile.getName());
    }

    @Override
    public SourceFile findByTableName(String tableName) {
        return sourceFileProxy.findByTableName(getShortTenantId(), tableName);
    }

    @Override
    public void copySourceFile(String originalSourceFileName, String targetTableName, String targetTenant) {
        sourceFileProxy.copySourceFile(getShortTenantId(), originalSourceFileName, targetTableName, targetTenant);
    }

    @Override
    public SourceFile getByTableNameAcrossTenant(String tableName) {
        return sourceFileProxy.findByTableNameCrossTenant(tableName);
    }

    @Override
    public SourceFile createSourceFileFromS3(FileProperty fileProperty, String entity) {
        return sourceFileProxy.createSourceFileFromS3(getShortTenantId(), fileProperty, entity, null);
    }

    private String getShortTenantId() {
        return MultiTenantContext.getShortTenantId();
    }
}
