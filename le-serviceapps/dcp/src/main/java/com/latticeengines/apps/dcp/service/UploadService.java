package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadDiagnostics;
import com.latticeengines.domain.exposed.dcp.UploadJobDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;

public interface UploadService {

    List<UploadDetails> getUploads(String customerSpace, String sourceId, Boolean includeConfig);

    List<UploadDetails> getUploads(String customerSpace, String sourceId, Upload.Status status, Boolean includeConfig);

    UploadDetails getUploadByUploadId(String customerSpace, String uploadId, Boolean includeConfig);

    UploadDetails createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig, String userId);

    void registerMatchResult(String customerSpace, String uploadId, String tableName);

    void updateUploadConfig(String customerSpace, String uploadId, UploadConfig uploadConfig);

    void updateUploadStatus(String customerSpace, String uploadId, Upload.Status status, UploadDiagnostics uploadDiagnostics);

    UploadStatsContainer appendStatistics(String uploadId, UploadStatsContainer container);

    void updateStatsWorkflowPid(String uploadId, Long statsTimestamp, Long workflowPid);

    void updateStatistics(String uploadId, Long statsTimestamp, UploadStats uploadStats);

    UploadDetails setLatestStatistics(String uploadId, Long statsTimestamp);

    String getMatchResultTableName(String uploadId);

    UploadJobDetails getJobDetailsByUploadId(String customerSpace, String uploadId);
}
