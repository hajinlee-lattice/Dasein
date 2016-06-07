package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public class SourceFilePurgeCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(SourceFilePurgeCallable.class);

    private SourceFileService sourceFileService;

    private SourceFileEntityMgr sourceFileEntityMgr;

    private TenantEntityMgr tenantEntityMgr;

    private Configuration yarnConfiguration;

    private int retainDays;

    public SourceFilePurgeCallable(Builder builder) {
        this.sourceFileService = builder.getSourceFileService();
        this.sourceFileEntityMgr = builder.getSourceFileEntityMgr();
        this.yarnConfiguration = builder.getYarnConfiguration();
        this.tenantEntityMgr = builder.getTenantEntityMgr();
        this.retainDays = builder.getRetainDays();
    }

    @Override
    public Boolean call() throws Exception {
        log.info("Begin purge old source files");
        List<SourceFile> allSourceFiles = sourceFileEntityMgr.findAllSourceFiles();
        if (allSourceFiles != null) {
            Date now = new Date(System.currentTimeMillis());
            for (SourceFile sourceFile : allSourceFiles) {
                if (tenantEntityMgr.findByTenantId(sourceFile.getTenant().getId()) == null) {
                    try {
                        HdfsUtils.rmdir(yarnConfiguration, sourceFile.getPath());
                    } catch (IOException e) {
                        log.error(e);
                    }
                    sourceFileService.delete(sourceFile);
                } else {
                    int diffIndaysCreate = (int) ((now.getTime() - sourceFile.getCreated()
                            .getTime()) / (1000 * 60 * 60 * 24));
                    int diffIndaysUpdate = (int) ((now.getTime() - sourceFile.getUpdated()
                            .getTime()) / (1000 * 60 * 60 * 24));
                    if (diffIndaysCreate > retainDays && diffIndaysUpdate > retainDays) {
                        try {
                            HdfsUtils.rmdir(yarnConfiguration, sourceFile.getPath());
                        } catch (IOException e) {
                            log.error(e);
                        }
                        sourceFileService.delete(sourceFile);
                    }
                }
            }
        }
        return true;
    }

    public static class Builder {
        private SourceFileService sourceFileService;
        private SourceFileEntityMgr sourceFileEntityMgr;
        private TenantEntityMgr tenantEntityMgr;
        private Configuration yarnConfiguration;
        private int retainDays;

        public Builder() {

        }

        public Builder sourceFileService(SourceFileService sourceFileService) {
            this.sourceFileService = sourceFileService;
            return this;
        }

        public Builder sourceFileEntityMgr(SourceFileEntityMgr sourceFileEntityMgr) {
            this.sourceFileEntityMgr = sourceFileEntityMgr;
            return this;
        }

        public Builder tenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
            this.tenantEntityMgr = tenantEntityMgr;
            return this;
        }

        public Builder Configuration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public Builder retainDays(int retainDays) {
            this.retainDays = retainDays;
            return this;
        }

        public SourceFileService getSourceFileService() {
            return sourceFileService;
        }

        public SourceFileEntityMgr getSourceFileEntityMgr() {
            return sourceFileEntityMgr;
        }

        public TenantEntityMgr getTenantEntityMgr() {
            return tenantEntityMgr;
        }

        public Configuration getYarnConfiguration() {
            return yarnConfiguration;
        }

        public int getRetainDays() {
            return retainDays;
        }
    }

}
