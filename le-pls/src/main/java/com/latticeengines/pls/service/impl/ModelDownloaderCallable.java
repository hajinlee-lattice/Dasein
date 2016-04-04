package com.latticeengines.pls.service.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.BlockMissingException;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

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
        String startingHdfsPoint = modelServiceHdfsBaseDir + "/" + CustomerSpace.parse(tenant.getId());
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
            log.info(String.format("%d file(s) downloaded from modeling service for tenant %s.", files.size(),
                    tenant.getId()));
        } catch (FileNotFoundException e) {
            log.warn(String.format("No models seem to have been created yet for tenant with id %s. Error message: %s",
                    tenant.getId(), e.getMessage()));
            return false;
        }

        Set<String> set = new HashSet<>();
        SecurityContextUtils.setTenant(tenant);
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
        for (ModelSummary summary : summaries) {
            try {
                set.add(UuidUtils.extractUuid(summary.getId()));
            } catch (Exception e) {
                // Skip any model summaries that have unexpected ID syntax
                log.warn(e);
            }
        }
        boolean foundFilesToDownload = false;

        for (String file : files) {
            try {
                String modelSummaryId = UuidUtils.extractUuid(file);
                if (!set.contains(modelSummaryId)) {
                    String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, file);
                    ModelSummary summary = parser.parse(file, contents);
                    String[] tokens = file.split("/");
                    summary.setTenant(tenant);
                    try {
                        summary.setApplicationId("application_" + tokens[tokens.length - 3]);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        log.error(String.format("Cannot set application id of model summary with id %s.",
                                modelSummaryId));
                    }
                    log.info(String.format("Creating model summary with id %s appId %s from file %s.", //
                            summary.getId(), summary.getApplicationId(), file));
                    modelSummaryEntityMgr.create(summary);

                    foundFilesToDownload = true;
                }
            } catch (BlockMissingException e) {
                log.error(e);
                // delete the bad model summary file
                HdfsUtils.rmdir(yarnConfiguration, file);
            } catch (IOException e) {
                // Will trigger PagerDuty alert
                log.fatal(ExceptionUtils.getFullStackTrace(e));
            } catch (Exception e) {
                log.error(e);
            }
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
