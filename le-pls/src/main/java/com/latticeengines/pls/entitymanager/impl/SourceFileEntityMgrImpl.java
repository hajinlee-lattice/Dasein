package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.SourceFileDao;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("sourceFileEntityMgr")
public class SourceFileEntityMgrImpl extends BaseEntityMgrImpl<SourceFile> implements SourceFileEntityMgr {

    private static final Logger log = Logger.getLogger(SourceFileEntityMgr.class);

    @Autowired
    private SourceFileDao sourceFileDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public BaseDao<SourceFile> getDao() {
        return sourceFileDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(SourceFile sourceFile) {
        Tenant tenant = tenantEntityMgr.findByTenantId(SecurityContextUtils.getTenant().getId());
        sourceFile.setTenant(tenant);
        sourceFile.setTenantId(tenant.getPid());
        sourceFile.setPid(null);
        super.create(sourceFile);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile findByName(String name) {
        return sourceFileDao.findByName(name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public SourceFile clone(String name) {
        CustomerSpace customerSpace = SecurityContextUtils.getCustomerSpace();

        SourceFile source = findByName(name);
        if (source == null) {
            throw new RuntimeException(String.format("No such SourceFile with name %s", name));
        }

        SourceFile clone = new SourceFile();
        clone.setName(makeCloneFileName(source.getName()));
        clone.setState(source.getState());
        clone.setDescription(source.getDescription());
        clone.setApplicationId(null);
        clone.setSchemaInterpretation(source.getSchemaInterpretation());

        if (source.getPath() != null) {
            Path filesPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                    SecurityContextUtils.getCustomerSpace());
            Path destPath = filesPath.append(clone.getName());
            clone.setPath(destPath.toString());
        }

        if (source.getTableName() != null) {
            Table table = metadataProxy.getTable(customerSpace.toString(), source.getTableName());
            table.setName(makeCloneFileName(table.getName()));
            clone.setTableName(table.getName());
            metadataProxy.createTable(customerSpace.toString(), table.getName(), table);
        }

        create(clone);

        Path tablesPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                SecurityContextUtils.getCustomerSpace());

        if (source.getTableName() != null) {
            Path sourcePath = tablesPath.append(source.getTableName());
            Path destPath = tablesPath.append(clone.getTableName());
            log.info(String.format("Copying table data from %s to %s", sourcePath, destPath));
            try {
                HdfsUtils.copyFiles(yarnConfiguration, sourcePath.toString(), destPath.toString());
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to copy in HDFS from %s to %s", sourcePath.toString(),
                        destPath.toString()), e);
            }
        }

        if (source.getPath() != null) {
            try {
                log.info(String.format("Copying source file data from %s to %s", source.getPath(), clone.getPath()));
                HdfsUtils.copyFiles(yarnConfiguration, source.getPath(), clone.getPath());
            } catch (Exception e) {
                // Cleanup if the second HDFS operation failed
                try {
                    if (clone.getTableName() != null) {
                        Path destPath = tablesPath.append(clone.getTableName());
                        HdfsUtils.rmdir(yarnConfiguration, destPath.toString());
                    }
                } catch (Exception e2) {
                    throw new RuntimeException(e2);
                }

                throw new RuntimeException(String.format("Failed to copy in HDFS from %s to %s", source.getPath(),
                        clone.getPath()), e);
            }
        }

        return clone;
    }

    private String makeCloneFileName(String fileName) {
        List<String> parts = Lists.newArrayList(fileName.split("\\."));
        if (parts.size() == 1) {
            return fileName + ".clone";
        }
        List<String> beginparts = parts.subList(0, parts.size() - 1);
        String begin = StringUtils.join(beginparts, '.');
        return begin + ".clone." + parts.get(parts.size() - 1);
    }
}
