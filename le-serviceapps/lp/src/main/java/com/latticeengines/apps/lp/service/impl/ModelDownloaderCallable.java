package com.latticeengines.apps.lp.service.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.aws.AwsApplicationId;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

public class ModelDownloaderCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(ModelDownloaderCallable.class);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");

    private Tenant tenant;
    private String modelServiceHdfsBaseDir;
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    private BucketedScoreService bucketedScoreService;
    private Configuration yarnConfiguration;
    private ModelSummaryParser parser;
    private FeatureImportanceParser featureImportanceParser;
    private Set<String> applicationFilters;
    private final Set<String> modelSummaryIds;

    public ModelDownloaderCallable(Builder builder) {
        this.tenant = builder.getTenant();
        this.modelServiceHdfsBaseDir = builder.getModelServiceHdfsBaseDir();
        this.bucketedScoreService = builder.getBucketedScoreService();
        this.modelSummaryEntityMgr = builder.getModelSummaryEntityMgr();
        this.yarnConfiguration = builder.getYarnConfiguration();
        this.parser = builder.getModelSummaryParser();
        this.featureImportanceParser = builder.getFeatureImportanceParser();
        this.modelSummaryIds = builder.getModelSummaryIds();
        this.applicationFilters = builder.getApplicationFilters();
    }

    @Override
    public Boolean call() throws Exception {
        String startingHdfsPoint = modelServiceHdfsBaseDir;
        if (!startingHdfsPoint.endsWith("/")) {
            startingHdfsPoint += "/";
        }
        startingHdfsPoint += CustomerSpace.parse(tenant.getId()) + "/models";
        final long acceptTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
        HdfsUtils.HdfsFileFilter filter = file -> {
            if (file == null) {
                return false;
            }

            if (file.getModificationTime() < acceptTime) {
                return false;
            }

            String name = file.getPath().getName();
            return name.equals("modelsummary.json");
        };

        HdfsUtils.HdfsFileFilter folderFilter = file -> {
            if (file == null) {
                return false;
            }
            return file.getModificationTime() >= acceptTime;
        };

        if (!HdfsUtils.fileExists(yarnConfiguration, startingHdfsPoint)) {
            log.debug(String.format("No models seem to have been created yet for tenant with id %s", tenant.getId()));
            return false;
        }

        List<String> files;
        try {
            long startTime = System.currentTimeMillis();
            files = HdfsUtils.getFilesForDirRecursiveWithFilterOnDir(yarnConfiguration, startingHdfsPoint, filter,
                    folderFilter);
            long recursiveGetFilesTime = System.currentTimeMillis() - startTime;
            if (recursiveGetFilesTime > 1000) {
                log.info(String.format("Recursive get files from %s duration: %d milliseconds", startingHdfsPoint,
                        recursiveGetFilesTime));
            }
            log.debug(String.format("%d file(s) downloaded from modeling service for tenant %s.", files.size(),
                    tenant.getId()));
        } catch (FileNotFoundException e) {
            log.warn(String.format("No models seem to have been created yet for tenant with id %s. Error message: %s",
                    tenant.getId(), e.getMessage()));
            return false;
        }

        MultiTenantContext.setTenant(tenant);

        boolean foundFilesToDownload = false;

        for (String file : files) {
            long startTime = System.currentTimeMillis();
            String constraintViolationId = StringUtils.EMPTY;
            try {
                String modelSummaryId = UuidUtils.parseUuid(file);
                String appId = UuidUtils.parseAppId(file);
                if (CollectionUtils.isNotEmpty(applicationFilters) && !applicationFilters.contains(appId)) {
                    log.info("appId " + appId + " is not in applicationFilters");
                    continue;
                }
                synchronized (modelSummaryIds) {
                    if (modelSummaryIds.contains(modelSummaryId)) {
                        log.info("modelSummaryId " + modelSummaryId + " already exists in modelSummaryIds.");
                        continue;
                    } else {
                        modelSummaryIds.add(modelSummaryId);
                    }
                }
                String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, file);
                long getHdfsFileContentsTime = System.currentTimeMillis() - startTime;
                log.info(String.format("Reading data from %s elapse %d milliseconds", file, getHdfsFileContentsTime));
                ModelSummary summary = parser.parse(file, contents);
                String[] tokens = file.split("/");
                summary.setTenant(tenant);
                try {
                    setFeatureImportance(summary, file);
                } catch (IOException e) {
                    log.warn("Errors fetching RF feature importance file. Skipping...");
                }

                try {
                    String coreId = tokens[tokens.length - 3];
                    String applicationId = "application_" + coreId;
                    if (AwsApplicationId.isAwsBatchJob(applicationId + "_aws")) {
                        applicationId = applicationId + "_aws";
                    }
                    summary.setApplicationId(applicationId);
                } catch (ArrayIndexOutOfBoundsException e) {
                    log.error(String.format("Cannot set application id of model summary with id %s.", modelSummaryId));
                }
                constraintViolationId = summary.getId();
                modelSummaryEntityMgr.create(summary);
                foundFilesToDownload = true;
                if (summary.getEventTableName().startsWith("copy_")) {
                    createBucketMetadatasForCopiedModel(summary.getId());
                }
                long totalTime = System.currentTimeMillis() - startTime;
                log.info(String.format(
                        "Creating model summary with id %s appId %s from file %s. Duration: %d milliseconds.", //
                        summary.getId(), summary.getApplicationId(), file, totalTime));
            } catch (BlockMissingException e) {
                log.error(e.getMessage(), e);
                // delete the bad model summary file
                HdfsUtils.rmdir(yarnConfiguration, file);
            } catch (IOException e) {
                // Will trigger PagerDuty alert
                log.error(fatal, "Failed to download model summary", e);
            } catch (ConstraintViolationException e) {
                log.info(String.format(
                        "Cannot create model summary with Id %s, constraint violation. Hdfs file: %s, TenantId: %s",
                        constraintViolationId, file, tenant.getId()));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return foundFilesToDownload;
    }

    private void createBucketMetadatasForCopiedModel(String copiedModelId) {
        ModelSummary copiedModelSummary = modelSummaryEntityMgr.getByModelId(copiedModelId);
        List<ModelSummary> modelSummaries = modelSummaryEntityMgr
                .getModelSummariesByApplicationId(copiedModelSummary.getApplicationId());
        ModelSummary originalModelSummary = null;
        for (ModelSummary modelSummary : modelSummaries) {
            if (!modelSummary.getEventTableName().startsWith("copy_")) {
                originalModelSummary = modelSummary;
            }
        }
        if (originalModelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18127, new String[] { copiedModelId });
        }
        List<BucketMetadata> bucketMetadatas = bucketedScoreService
                .getABCDBucketsByModelGuidAcrossTenant(originalModelSummary.getId());
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setModelGuid(copiedModelId);
        request.setBucketMetadataList(copyBucketMetadatasForCopiedModel(bucketMetadatas, copiedModelId));
        bucketedScoreService.createABCDBuckets(request);
    }

    @SuppressWarnings("deprecation")
    private List<BucketMetadata> copyBucketMetadatasForCopiedModel(List<BucketMetadata> originalBucketMetadatas,
            String copiedModelId) {
        List<BucketMetadata> bucketMetadatas = new ArrayList<>();

        for (BucketMetadata originalBucketMetadata : originalBucketMetadatas) {
            BucketMetadata bucketMetadata = new BucketMetadata();

            bucketMetadata.setBucket(originalBucketMetadata.getBucket());
            bucketMetadata.setLeftBoundScore(originalBucketMetadata.getLeftBoundScore());
            bucketMetadata.setRightBoundScore(originalBucketMetadata.getRightBoundScore());
            bucketMetadata.setNumLeads(originalBucketMetadata.getNumLeads());
            bucketMetadata.setLift(originalBucketMetadata.getLift());
            bucketMetadata.setLastModifiedByUser(originalBucketMetadata.getLastModifiedByUser());
            bucketMetadata.setModelSummary(modelSummaryEntityMgr.getByModelId(copiedModelId));

            bucketMetadatas.add(bucketMetadata);
        }

        return bucketMetadatas;
    }

    private void setFeatureImportance(ModelSummary summary, String modelSummaryHdfsPath) throws IOException {
        String fiPath = getRandomForestFiHdfsPath(modelSummaryHdfsPath);
        Map<String, Double> fiMap = featureImportanceParser.parse(fiPath, //
                HdfsUtils.getHdfsFileContents(yarnConfiguration, fiPath));

        List<Predictor> predictors = summary.getPredictors();
        Map<String, Predictor> map = new HashMap<>();
        for (Predictor predictor : predictors) {
            map.put(predictor.getName(), predictor);
        }
        for (Map.Entry<String, Double> entry : fiMap.entrySet()) {
            Predictor p = map.get(entry.getKey());

            if (p != null) {
                p.setFeatureImportance(entry.getValue());
            }
        }
    }

    private static String getRandomForestFiHdfsPath(String modelSummaryHdfsPath) {
        String[] tokens = modelSummaryHdfsPath.split("/");
        String[] rfModelTokens = new String[tokens.length - 1];
        System.arraycopy(tokens, 0, rfModelTokens, 0, rfModelTokens.length - 1);
        rfModelTokens[rfModelTokens.length - 1] = "rf_model.txt";
        return StringUtils.join(rfModelTokens, "/");
    }

    public static class Builder {
        private Tenant tenant;
        private String modelServiceHdfsBaseDir;
        private ModelSummaryEntityMgr modelSummaryEntityMgr;
        private BucketedScoreService bucketedScoreService;
        private Configuration yarnConfiguration;
        private ModelSummaryParser modelSummaryParser;
        private FeatureImportanceParser featureImportanceParser;
        private Set<String> modelSummaryIds;
        private Set<String> applicationFilters;


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

        public Builder bucketedScoreService(BucketedScoreService bucketedScoreService) {
            this.bucketedScoreService = bucketedScoreService;
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

        public Builder featureImportanceParser(FeatureImportanceParser featureImportanceParser) {
            this.featureImportanceParser = featureImportanceParser;
            return this;
        }

        public Builder modelSummaryIds(Set<String> modelSummaryIds) {
            this.modelSummaryIds = modelSummaryIds;
            return this;
        }

        public Builder applicationFilters(Set<String> applicationFilters) {
            this.applicationFilters = applicationFilters;
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

        public BucketedScoreService getBucketedScoreService() {
            return bucketedScoreService;
        }

        public Configuration getYarnConfiguration() {
            return yarnConfiguration;
        }

        public ModelSummaryParser getModelSummaryParser() {
            return modelSummaryParser;
        }

        public FeatureImportanceParser getFeatureImportanceParser() {
            return featureImportanceParser;
        }

        public Set<String> getModelSummaryIds() {
            return modelSummaryIds;
        }

        public Set<String> getApplicationFilters() { return  applicationFilters; }
    }

}
