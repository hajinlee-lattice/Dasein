package com.latticeengines.pls.service.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.MetadataSegmentExportCleanupService;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("metadataSegmentExportCleanupService")
public class MetadataSegmentExportCleanupServiceImpl implements MetadataSegmentExportCleanupService {

    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentExportCleanupServiceImpl.class);

    @Autowired
    private MetadataSegmentExportService metadataSegmentExportService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void cleanup() {

        List<Tenant> tenants = tenantEntityMgr.findAll();
        if (CollectionUtils.isNotEmpty(tenants)) {
            tenants.stream() //
                    .forEach(tenant -> {
                        MultiTenantContext.setTenant(tenant);

                        List<MetadataSegmentExport> exportJobs = metadataSegmentExportService.getSegmentExports();

                        if (CollectionUtils.isNotEmpty(exportJobs)) {
                            exportJobs.stream() //
                                    .forEach(exportJob -> {
                                        if (exportJob.getCleanupBy() != null && exportJob.getCleanupBy().getTime() < System.currentTimeMillis()) {
                                            cleanupExportJob(tenant, exportJob);
                                        }
                                    });
                        }
                        MultiTenantContext.setTenant(null);
                    });
        }
    }

    public void cleanupExportJob(Tenant tenant, MetadataSegmentExport exportJob) {
        try {
            log.info(String.format("Trying to cleanup old export job for tenant: %s, job: %s", tenant.getId(),
                    JsonUtils.serialize(exportJob)));

            String tableName = exportJob.getTableName();
            metadataProxy.deleteTable(tenant.getId(), tableName);

            String exportedFilePath = metadataSegmentExportService.getExportedFilePath(exportJob);

            if (HdfsUtils.fileExists(yarnConfiguration, exportedFilePath)) {
                HdfsUtils.rmdir(yarnConfiguration, exportedFilePath);
                HdfsUtils.rmdir(yarnConfiguration, exportJob.getPath());
            }

            metadataSegmentExportService.deleteSegmentExportByExportId(exportJob.getExportId());
        } catch (Exception ex) {
            log.error(String.format("Could not cleanup old export job for tenant: %s, job: %s", tenant.getId(),
                    JsonUtils.serialize(exportJob)), ex);
        }
    }
}
