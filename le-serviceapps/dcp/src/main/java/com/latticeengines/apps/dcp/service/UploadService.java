package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;

public interface UploadService {

    List<UploadDetails> getUploads(String customerSpace, String sourceId);

    List<UploadDetails> getUploads(String customerSpace, String sourceId, Upload.Status status);

    UploadDetails getUploadByUploadId(String customerSpace, String uploadId);

    UploadDetails createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig);

    void registerMatchResult(String customerSpace, String uploadId, String tableName);

    void registerMatchCandidates(String customerSpace, String uploadId, String tableName);

    void updateUploadConfig(String customerSpace, String uploadId, UploadConfig uploadConfig);

    void updateUploadStatus(String customerSpace, String uploadId, Upload.Status status);

    UploadStatsContainer appendStatistics(String uploadId, UploadStatsContainer container);

    void updateStatsWorkflowPid(String uploadId, Long statsTimestamp, Long workflowPid);

    void updateStatistics(String uploadId, Long statsTimestamp, UploadStats uploadStats);

    UploadDetails setLatestStatistics(String uploadId, Long statsTimestamp);
}
