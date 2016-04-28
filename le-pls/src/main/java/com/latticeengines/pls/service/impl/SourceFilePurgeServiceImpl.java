package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.SourceFilePurgeService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@DisallowConcurrentExecution
@Component("sourceFilePurgeService")
public class SourceFilePurgeServiceImpl extends QuartzJobBean implements SourceFilePurgeService {
    
    private static final Log log = LogFactory.getLog(SourceFilePurgeServiceImpl.class);
    
    private SourceFileService sourceFileService;
    
    private SourceFileEntityMgr sourceFileEntityMgr;
    
    private TenantEntityMgr tenantEntityMgr;
    
    private Configuration yarnConfiguration;
    
    private int retainDays;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
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
                    int diffIndaysCreate = (int)((now.getTime() - sourceFile.getCreated().getTime()) / (1000 * 60 * 60 * 24));
                    int diffIndaysUpdate = (int)((now.getTime() - sourceFile.getUpdated().getTime()) / (1000 * 60 * 60 * 24));
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
    }
    
    public Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }
    
    public SourceFileService getSourceFileService() {
        return sourceFileService;
    }
    
    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }
    
    public SourceFileEntityMgr getSourceFileEntityMgr() {
        return sourceFileEntityMgr;
    }
    
    public void setSourceFileEntityMgr(SourceFileEntityMgr sourceFileEntityMgr) {
        this.sourceFileEntityMgr = sourceFileEntityMgr;
    }
    
    public TenantEntityMgr getTenantEntityMgr() {
        return tenantEntityMgr;
    }

    public void setTenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
        this.tenantEntityMgr = tenantEntityMgr;
    }
    
    public int getRetainDays() {
        return retainDays;
    }
    
    public void setRetainDays(int retainDays) {
        this.retainDays = retainDays;
    }

}
