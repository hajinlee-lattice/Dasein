package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AtlasExportEntityMgr;
import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("atlasExportService")
public class AtlasExportServiceImpl implements AtlasExportService {

    private static Logger log = LoggerFactory.getLogger(AtlasExportServiceImpl.class);

    @Inject
    private AtlasExportEntityMgr atlasExportEntityMgr;

    @Inject
    private YarnConfiguration yarnConfiguration;

    private ExecutorService service = ThreadPoolUtils.getSingleThreadPool("cleanup-export");

    @Override
    public AtlasExport createAtlasExport(String customerSpace, AtlasExportType exportType) {
        return atlasExportEntityMgr.createAtlasExport(exportType);
    }
    @Override
    public void deleteAtlasExport(String customerSpace, String uuid) {
        atlasExportEntityMgr.deleteByExportId(uuid);
    }

    @Override
    public void addFileToSystemPath(String customerSpace, String uuid, String fileName, List<String> filesToDelete) {
        AtlasExport atlasExport = getAtlasWithPath(uuid, fileName, false);
        atlasExport.setFilesToDelete(filesToDelete);
        atlasExportEntityMgr.update(atlasExport);
    }

    private AtlasExport getAtlasWithPath(String uuid, String fileName, boolean dropfoler) {
        AtlasExport atlasExport = atlasExportEntityMgr.findByUuid(uuid);
        if (atlasExport == null) {
            log.error("Cannot find Atlas Export Object with uuid: " + uuid);
            throw new IllegalArgumentException("Cannot find Atlas Export Object with uuid: " + uuid);
        }
        List<String> fileList = dropfoler ? atlasExport.getFilesUnderDropFolder() : atlasExport.getFilesUnderSystemPath();
        if (fileList == null) {
            fileList = new ArrayList<>();
        }
        if (!fileList.contains(fileName)) {
            fileList.add(fileName);
        }
        if (dropfoler) {
            atlasExport.setFilesUnderDropFolder(fileList);
        } else {
            atlasExport.setFilesUnderSystemPath(fileList);
        }
        return atlasExport;
    }

    // file name is with path
    @Override
    public void addFileToDropFolder(String customerSpace, String uuid, String fileName, List<String> filesToDelete) {
        AtlasExport atlasExport = getAtlasWithPath(uuid, fileName, true);
        atlasExport.setFilesToDelete(filesToDelete);
        atlasExportEntityMgr.update(atlasExport);
    }

    @Override
    public AtlasExport getAtlasExport(String customerSpace, String uuid) {
        return atlasExportEntityMgr.findByUuid(uuid);
    }

    @Override
    public List<AtlasExport> findAll(String customerSpace) {
        List<AtlasExport> atlasExports = atlasExportEntityMgr.findAll();
        List<AtlasExport> result = new ArrayList<>();
        List<AtlasExport> cleanupList = new ArrayList<>();
        atlasExports.stream().forEach(exportJob -> {
            if (exportJob.getCleanupBy().getTime() < System.currentTimeMillis()) {
                cleanupList.add(exportJob);
            } else {
                result.add(exportJob);
            }
        });
        if (CollectionUtils.isNotEmpty(cleanupList)) {
            Tenant tenant = MultiTenantContext.getTenant();
            service.submit(new ClearExportJob(tenant, cleanupList));
        }
        return result;
    }

    @Override
    public void updateAtlasExport(String customerSpace, AtlasExport atlasExport) {
        atlasExportEntityMgr.createOrUpdate(atlasExport);
    }

    @Override
    public AtlasExport createAtlasExport(String customerSpace, AtlasExport atlasExport) {
        return atlasExportEntityMgr.createAtlasExport(atlasExport);
    }

    private void cleanUpAtlasExport(AtlasExport atlasExport) {
        if (CollectionUtils.isNotEmpty(atlasExport.getFilesToDelete())) {
            try {
                for (String path : atlasExport.getFilesToDelete()) {
                    if (HdfsUtils.fileExists(yarnConfiguration, path)) {
                        HdfsUtils.rmdir(yarnConfiguration, path);
                    }
                }
                atlasExportEntityMgr.deleteByExportId(atlasExport.getUuid());
            } catch (Exception ex) {
                log.error(String.format("Could not cleanup export job for job: %s", JsonUtils.serialize(atlasExport)), ex);
            }
        }
    }

    private class ClearExportJob implements Runnable {

        private Tenant tenant;

        private List<AtlasExport> atlasExports;

        ClearExportJob(Tenant tenant, List<AtlasExport> atlasExports) {
            this.tenant = tenant;
            this.atlasExports = atlasExports;
        }

        @Override
        public void run() {
            MultiTenantContext.setTenant(tenant);
            atlasExports.stream().forEach(atlasExport -> cleanUpAtlasExport(atlasExport));
            MultiTenantContext.setTenant(null);
        }
    }
}
