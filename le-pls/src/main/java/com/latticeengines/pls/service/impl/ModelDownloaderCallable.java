package com.latticeengines.pls.service.impl;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;

public class ModelDownloaderCallable implements Callable<Boolean> {
    
    private static final Log log = LogFactory.getLog(ModelDownloaderCallable.class);
    
    private Tenant tenant;
    private String modelServiceHdfsBaseDir;
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    private Configuration yarnConfiguration;
    private ModelSummaryParser parser;
    
    public ModelDownloaderCallable(Builder builder) {
        this.tenant = builder.getTenant();
        this.modelServiceHdfsBaseDir = builder.getModelServiceHdfsBaseDir();
        this.modelSummaryEntityMgr = builder.getModelSummaryEntityMgr();
        this.yarnConfiguration = builder.getYarnConfiguration();
        this.parser = builder.getModelSummaryParser();
    }

    @Override
    public Boolean call() throws Exception {
        String startingHdfsPoint = modelServiceHdfsBaseDir + "/" + tenant.getId();
        final Long tenantRegistrationTime = tenant.getRegisteredTime();
        HdfsUtils.HdfsFileFilter filter = new HdfsUtils.HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }
                
                if (file.getModificationTime() < tenantRegistrationTime) {
                    return false;
                }
                
                String name = file.getPath().getName().toString();
                return name.equals("modelsummary.json");
            }
            
        };
        
        List<String> files = new ArrayList<>();
        try {
            files = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, startingHdfsPoint, filter);
        } catch (FileNotFoundException e) {
            log.warn("No models seem to have been created yet for this tenant.", e);
            return false;
        }
        
        
        
        boolean foundFilesToDownload = false;
        try {
            for (String file : files) {
                String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, file);
                ModelSummary summary = parser.parse(file, contents);
                summary.setTenant(tenant);
                modelSummaryEntityMgr.create(summary);
                foundFilesToDownload = true;
            }
        } catch (Exception e) {
            log.error(e);
        }
        
        return foundFilesToDownload;
    }
    
    public static class Builder {
        private Tenant tenant;
        private String modelServiceHdfsBaseDir;
        private ModelSummaryEntityMgr modelSummaryEntityMgr;
        private Configuration yarnConfiguration;
        private ModelSummaryParser modelSummaryParser;
        
        public Builder() {
            
        }
        
        public Builder tenant(Tenant tenant) {
            this.tenant = tenant;
            return this;
        }
        
        public Builder modelServiceHdfsBaseDir(String modelServiceHdfsBaseDir) {
            this.modelServiceHdfsBaseDir = modelServiceHdfsBaseDir;
            return this;
        }
        
        public Builder modelSummaryEntityMgr(ModelSummaryEntityMgr modelSummaryEntityMgr) {
            this.modelSummaryEntityMgr = modelSummaryEntityMgr;
            return this;
        }
        
        public Builder yarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public Builder modelSummaryParser(ModelSummaryParser modelSummaryParser) {
            this.modelSummaryParser = modelSummaryParser;
            return this;
        }

        public Tenant getTenant() {
            return tenant;
        }

        public String getModelServiceHdfsBaseDir() {
            return modelServiceHdfsBaseDir;
        }

        public ModelSummaryEntityMgr getModelSummaryEntityMgr() {
            return modelSummaryEntityMgr;
        }
        
        public Configuration getYarnConfiguration() {
            return yarnConfiguration;
        }
        
        public ModelSummaryParser getModelSummaryParser() {
            return modelSummaryParser;
        }

    }

}
