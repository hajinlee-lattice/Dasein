package com.latticeengines.pls.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.SourceFileService;

@Component("sourceFileService")
public class SourceFileServiceImpl implements SourceFileService {
    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public SourceFile findByName(String name) {
        return sourceFileEntityMgr.findByName(name);
    }

    @Override
    public SourceFile findByApplicationId(String applicationId) {
        return sourceFileEntityMgr.findByApplicationId(applicationId);
    }

    @Override
    public void create(SourceFile sourceFile) {
        sourceFileEntityMgr.create(sourceFile);
    }

    @Override
    public void update(SourceFile sourceFile) {
        SourceFile existing = sourceFileEntityMgr.findByName(sourceFile.getName());
        if (existing != null) {
            delete(existing);
        }
        sourceFileEntityMgr.create(sourceFile);
    }

    @Override
    public void delete(SourceFile sourceFile) {
        sourceFileEntityMgr.delete(sourceFile);
    }

    @Override
    public SourceFile findByTableName(String tableName) {
        return sourceFileEntityMgr.findByTableName(tableName);
    }

    @Override
    public void copySourceFile(Table table, SourceFile originalSourceFile, Tenant targetTenant) {
        String outputFileName = "file_" + table.getName();
        String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(targetTenant.getId())).toString()
                + "/" + outputFileName;
        try {
            HdfsUtils.copyFiles(yarnConfiguration, originalSourceFile.getPath(), outputPath);
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
        file.setTableName(table.getName());
        file.setTenant(targetTenant);
        create(file);

    }
}
