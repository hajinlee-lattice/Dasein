package com.latticeengines.apps.dcp.service.impl;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.dcp.entitymgr.UpdateStatisticsEntityMgr;
import com.latticeengines.apps.dcp.entitymgr.UploadEntityMgr;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadDiagnostics;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Service("uploadService")
public class UploadServiceImpl implements UploadService {

    private static final Logger log = LoggerFactory.getLogger(UploadServiceImpl.class);

    private static final String RANDOM_UPLOAD_ID_PATTERN = "Upload_%s";
    private static final int MAX_PAGE_SIZE = 100;

    @Inject
    private UploadEntityMgr uploadEntityMgr;

    @Inject
    private UpdateStatisticsEntityMgr statisticsEntityMgr;

    @Inject
    private MetadataService metadataService;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Override
    public boolean hasUnterminalUploads(String customerSpace, String excludeUploadId) {
        Set<Upload.Status> statuses = uploadEntityMgr.findAllStatusesExcludeOne(excludeUploadId);
        log.info("upload statuses : " + statuses);
        statuses.removeAll(Upload.Status.getTerminalStatuses());
        return CollectionUtils.isNotEmpty(statuses);
    }

    @Override
    public List<UploadDetails> getUploads(String customerSpace, String sourceId, Boolean includeConfig) {
        return getUploads(customerSpace, sourceId, includeConfig, 0, MAX_PAGE_SIZE);
    }

    @Override
    public List<UploadDetails> getUploads(String customerSpace, String sourceId, Boolean includeConfig, int pageIndex, int pageSize) {
        PageRequest pageRequest = getPageRequest(pageIndex, pageSize);
        List<Upload> uploads = expandStatistics(uploadEntityMgr.findBySourceId(sourceId, pageRequest));
        return uploads.stream().map(upload -> getUploadDetails(upload, includeConfig)).collect(Collectors.toList());
    }

    @Override
    public List<UploadDetails> getUploads(String customerSpace, String sourceId, Upload.Status status, Boolean includeConfig) {
        return getUploads(customerSpace, sourceId, status, includeConfig, 0, MAX_PAGE_SIZE);
    }

    @Override
    public List<UploadDetails> getUploads(String customerSpace, String sourceId, Upload.Status status, Boolean includeConfig, int pageIndex, int pageSize) {
        PageRequest pageRequest = getPageRequest(pageIndex, pageSize);
        List<Upload> uploads = expandStatistics(uploadEntityMgr.findBySourceIdAndStatus(sourceId, status, pageRequest));
        return uploads.stream().map(upload -> getUploadDetails(upload, includeConfig)).collect(Collectors.toList());
    }

    @Override
    public Long getUploadsCount(String customerSpace, String sourceId) {
        return uploadEntityMgr.countBySourceId(sourceId);
    }

    @Override
    public Long getUploadsCount(String customerSpace, String sourceId, Upload.Status status) {
        return uploadEntityMgr.countBySourceIdAndStatus(sourceId, status);
    }

    @Override
    public UploadDetails createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig, String userId) {
        if (StringUtils.isEmpty(sourceId)) {
            throw new RuntimeException("Cannot create upload bind with empty sourceId!");
        }
        String uploadId = generateRandomUploadId();
        Upload upload = new Upload();
        upload.setUploadId(uploadId);
        upload.setDisplayName(retrieveDisplayName(customerSpace, uploadConfig));
        upload.setSourceId(sourceId);
        upload.setCreatedBy(userId);
        upload.setTenant(MultiTenantContext.getTenant());
        upload.setStatus(Upload.Status.NEW);
        upload.setUploadConfig(uploadConfig);
        upload.setProgressPercentage(0.0);
        uploadEntityMgr.create(upload);

        return getUploadDetails(upload, Boolean.TRUE);
    }

    @Override
    public void registerMatchResult(String customerSpace, String uploadId, String tableName) {
        Upload upload = uploadEntityMgr.findByUploadId(uploadId);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with Pid: " + uploadId);
        }
        Table table = metadataService.getTable(CustomerSpace.parse(customerSpace), tableName);
        if (table == null) {
            throw new RuntimeException("Cannot find Upload match result table with name: " + tableName);
        }
        boolean hasOldTable = false;
        String oldTableName = getMatchResultTableName(customerSpace, uploadId);
        if (StringUtils.isNotBlank(oldTableName)) {
            hasOldTable = true;
        }
        upload.setMatchResult(table);
        uploadEntityMgr.update(upload);
        if (hasOldTable) {
            log.info("There was an old match result table {}, going to be marked as temporary.", oldTableName);
            RetentionPolicy retentionPolicy = RetentionPolicyUtil.toRetentionPolicy(7, RetentionPolicyTimeUnit.DAY);
            metadataService.updateTableRetentionPolicy(CustomerSpace.parse(customerSpace), oldTableName,
                    retentionPolicy);
        } else {
            log.info("There was no old match result table.");
        }
    }

    @Override
    public void updateUploadConfig(String customerSpace, String uploadId, UploadConfig uploadConfig) {
        Upload upload = uploadEntityMgr.findByUploadId(uploadId);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with UploadId: " + uploadId);
        }
        upload.setUploadConfig(uploadConfig);
        upload.setDisplayName(retrieveDisplayName(customerSpace, uploadConfig));
        uploadEntityMgr.update(upload);
    }

    @Override
    public void updateUploadStatus(String customerSpace, String uploadId, Upload.Status status,
                                   UploadDiagnostics uploadDiagnostics) {
        Upload upload = uploadEntityMgr.findByUploadId(uploadId);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with UploadId: " + uploadId);
        }
        upload.setStatus(status);
        if(uploadDiagnostics != null) {
            upload.setUploadDiagnostics(uploadDiagnostics);
        }
        uploadEntityMgr.update(upload);
    }

    @Override
    public UploadStatsContainer appendStatistics(String uploadId, UploadStatsContainer container) {
        Upload upload = uploadEntityMgr.findByUploadId(uploadId);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with UploadId: " + uploadId);
        }
        container.setUpload(upload);
        return statisticsEntityMgr.save(container);
    }

    @Override
    public void updateStatsWorkflowPid(String uploadId, Long statsId, Long workflowPid) {
        UploadStatsContainer container = findStats(uploadId, statsId);
        container.setWorkflowPid(workflowPid);
        statisticsEntityMgr.save(container);
    }

    @Override
    public void updateStatistics(String uploadId, Long statsId, UploadStats uploadStats) {
        UploadStatsContainer container = findStats(uploadId, statsId);
        container.setStatistics(uploadStats);
        statisticsEntityMgr.save(container);
    }

    @Override
    public UploadDetails setLatestStatistics(String uploadId, Long statsId) {
        Upload upload = uploadEntityMgr.findByUploadId(uploadId);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with UploadId: " + uploadId);
        }
        UploadStatsContainer container = statisticsEntityMgr.findByUploadAndId(upload, statsId);
        if (container == null) {
            throw new RuntimeException(
                    "Cannot find a statistics with id " + statsId + " for upload " + uploadId);
        }
        UploadStatsContainer currentLatest = statisticsEntityMgr.findIsLatest(upload);
        if (currentLatest != null) {
            currentLatest.setIsLatest(false);
            statisticsEntityMgr.save(currentLatest);
        }
        statisticsEntityMgr.setAsLatest(container);
        upload.setStatistics(container.getStatistics());
        return getUploadDetails(upload, Boolean.TRUE);
    }

    @Override
    public UploadDetails getUploadByUploadId(String customerSpace, String uploadId, Boolean includeConfig) {
        Upload upload = expandStatistics(uploadEntityMgr.findByUploadId(uploadId));
        UploadDetails details = getUploadDetails(upload, includeConfig);
        String appId = details.getUploadDiagnostics().getApplicationId();
        if (StringUtils.isNotBlank(appId) && ApplicationIdUtils.isFakeApplicationId(appId)) {
            boolean is5MinAgo = upload.getCreated().getTime() < //
                    System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);
            if (is5MinAgo) {
                log.info("Upload {} has a fake app id {}, trying to update it to the true app id.", uploadId, appId);
                Job job = workflowProxy.getWorkflowJobFromApplicationId(appId, customerSpace);
                String newId = job.getApplicationId();
                if (!ApplicationIdUtils.isFakeApplicationId(newId)) {
                    log.info("Updating app id for upload {} from {} to {}", uploadId, appId, newId);
                    details.getUploadDiagnostics().setApplicationId(newId);
                    uploadEntityMgr.update(upload);
                }
            }
        }
        return details;
    }

    @Override
    public String getMatchResultTableName(String customerSpace, String uploadId) {
        return uploadEntityMgr.findMatchResultTableNameByUploadId(uploadId);
    }

    @Override
    public void updateDropFileTime(String customerSpace, String uploadId, long dropFileTime) {
        Upload upload = uploadEntityMgr.findByUploadId(uploadId);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with UploadId: " + uploadId);
        }
        if (dropFileTime < 0) {
            throw new RuntimeException("Invalid dropFileTime provided: " + dropFileTime);
        }
        upload.setDropFileTime(new Date(dropFileTime));
        uploadEntityMgr.update(upload);
    }

    @Override
    public void updateProgressPercentage(String customerSpace, String uploadId, String progressPercentage) {
        Upload upload = uploadEntityMgr.findByUploadId(uploadId);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with UploadId: " + uploadId);
        }
        if (StringUtils.isBlank(progressPercentage)) {
            throw new RuntimeException("progressPercentage provided was null or empty: " + progressPercentage);
        }
        upload.setProgressPercentage(Double.valueOf(progressPercentage));
        uploadEntityMgr.update(upload);
    }

    @Override
    public void hardDeleteUploadUnderSource(String customerSpace, String sourceId) {
        PageRequest pageRequest = getPageRequest(0, MAX_PAGE_SIZE);
        List<Upload> uploads = expandStatistics(uploadEntityMgr.findBySourceId(sourceId, pageRequest));
        uploads.forEach(upload -> {
            uploadEntityMgr.delete(upload);
        });
    }

    private UploadStatsContainer findStats(String uploadId, Long statsId) {
        Upload upload = uploadEntityMgr.findByUploadId(uploadId);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with UploadId: " + uploadId);
        }
        UploadStatsContainer container = statisticsEntityMgr.findByUploadAndId(upload, statsId);
        if (container == null) {
            throw new RuntimeException(
                    "Cannot find a statistics with id " + statsId + " for upload " + uploadId);
        }
        return container;
    }

    private Upload expandStatistics(Upload upload) {
        UploadStatsContainer container = statisticsEntityMgr.findIsLatest(upload);
        if (container != null) {
            upload.setStatistics(container.getStatistics());
        }
        return upload;
    }

    private List<Upload> expandStatistics(Collection<Upload> uploads) {
        return uploads.stream().map(this::expandStatistics).collect(Collectors.toList());
    }

    private String generateRandomUploadId() {
        String randomUploadId = String.format(RANDOM_UPLOAD_ID_PATTERN,
                RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        while (uploadEntityMgr.findByUploadId(randomUploadId) != null) {
            randomUploadId = String.format(RANDOM_UPLOAD_ID_PATTERN,
                    RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        return randomUploadId;
    }

    private UploadDetails getUploadDetails(Upload upload, Boolean includeConfig) {
        UploadDetails details = new UploadDetails();
        details.setUploadId(upload.getUploadId());
        details.setDisplayName(upload.getDisplayName());
        details.setStatistics(upload.getStatistics());
        details.setStatus(upload.getStatus());
        if (upload.getUploadDiagnostics() != null) {
            details.setUploadDiagnostics(upload.getUploadDiagnostics());
        } else {
            details.setUploadDiagnostics(new UploadDiagnostics());
        }
        if (includeConfig) {
            details.setUploadConfig(upload.getUploadConfig());
        }
        details.setSourceId(upload.getSourceId());
        details.setUploadCreatedTime(upload.getCreated().getTime());
        details.setCreatedBy(upload.getCreatedBy());
        if (upload.getDropFileTime() != null) {
            details.setDropFileTime(upload.getDropFileTime().getTime());
        }
        details.setProgressPercentage(upload.getProgressPercentage());
        return details;
    }

    private String retrieveDisplayName(String customerSpace, UploadConfig config) {
        if (config == null) {
            return null;
        }

        if (config.getDropFilePath() != null) {
            String fileId = config.getDropFilePath();
            fileId = fileId.substring(fileId.lastIndexOf('/') + 1);
            SourceFile file = sourceFileProxy.findByName(customerSpace, fileId);
            return file != null ? file.getDisplayName() : fileId;
        } else {
            String rawFile = config.getUploadRawFilePath();
            if (rawFile != null) {
                rawFile = rawFile.substring(rawFile.lastIndexOf('/') + 1);
            }
            return rawFile;
        }
    }

    private PageRequest getPageRequest(int pageIndex, int pageSize) {
        Preconditions.checkState(pageIndex >= 0);
        Preconditions.checkState(pageSize > 0);
        pageSize = Math.min(pageSize, MAX_PAGE_SIZE);
        Sort sort = Sort.by(Sort.Direction.DESC, "updated");
        return PageRequest.of(pageIndex, pageSize, sort);
    }
}
