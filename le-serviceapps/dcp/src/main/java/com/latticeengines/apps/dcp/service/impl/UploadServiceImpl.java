package com.latticeengines.apps.dcp.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Service("uploadService")
public class UploadServiceImpl implements UploadService {

    private static final Logger log = LoggerFactory.getLogger(UploadServiceImpl.class);

    private static final String RANDOM_UPLOAD_ID_PATTERN = "Upload_%s";

    @Inject
    private UploadEntityMgr uploadEntityMgr;

    @Inject
    private UpdateStatisticsEntityMgr statisticsEntityMgr;

    @Inject
    private MetadataService metadataService;

    @Inject
    private WorkflowProxy workflowProxy;

    @Override
    public List<UploadDetails> getUploads(String customerSpace, String sourceId) {
        List<Upload> uploads = expandStatistics(uploadEntityMgr.findBySourceId(sourceId));
        return uploads.stream().map(this::getUploadDetails).collect(Collectors.toList());
    }

    @Override
    public List<UploadDetails> getUploads(String customerSpace, String sourceId, Upload.Status status) {
        List<Upload> uploads = expandStatistics(uploadEntityMgr.findBySourceIdAndStatus(sourceId, status));
        return uploads.stream().map(this::getUploadDetails).collect(Collectors.toList());
    }

    @Override
    public UploadDetails createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig) {
        if (StringUtils.isEmpty(sourceId)) {
            throw new RuntimeException("Cannot create upload bind with empty sourceId!");
        }
        String uploadId = generateRandomUploadId();
        Upload upload = new Upload();
        upload.setUploadId(uploadId);
        upload.setSourceId(sourceId);
        upload.setTenant(MultiTenantContext.getTenant());
        upload.setStatus(Upload.Status.NEW);
        upload.setUploadConfig(uploadConfig);
        uploadEntityMgr.create(upload);

        return getUploadDetails(upload);
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
        String oldTableName = getMatchResultTableName(uploadId);
        if (StringUtils.isNotBlank(oldTableName)) {
            Table oldTable = metadataService.getTable(CustomerSpace.parse(customerSpace), oldTableName);
            hasOldTable = oldTable != null;
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
        uploadEntityMgr.update(upload);
    }

    @Override
    public void updateUploadStatus(String customerSpace, String uploadId, Upload.Status status, UploadDiagnostics uploadDiagnostics) {
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
        return getUploadDetails(upload);
    }

    @Override
    public UploadDetails getUploadByUploadId(String customerSpace, String uploadId) {
        Upload upload = expandStatistics(uploadEntityMgr.findByUploadId(uploadId));
        return getUploadDetails(upload);
    }

    @Override
    public String getMatchResultTableName(String uploadId) {
        return uploadEntityMgr.findMatchResultTableNameByUploadId(uploadId);
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

    private UploadDetails getUploadDetails(Upload upload) {
        UploadDetails details = new UploadDetails();
        details.setUploadId(upload.getUploadId());
        details.setStatistics(upload.getStatistics());
        details.setStatus(upload.getStatus());
        if(upload.getUploadDiagnostics() != null) {
            details.setUploadDiagnostics(upload.getUploadDiagnostics());
        } else {
            details.setUploadDiagnostics(new UploadDiagnostics());
        }
        details.setUploadConfig(upload.getUploadConfig());
        details.setSourceId(upload.getSourceId());
        return details;
    }
}
