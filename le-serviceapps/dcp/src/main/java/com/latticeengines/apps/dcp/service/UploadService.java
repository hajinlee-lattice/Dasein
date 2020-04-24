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

    Upload getUpload(String customerSpace, Long pid);

    Upload createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig);

    void registerMatchResult(String customerSpace, long uploadPid, String tableName);

    void registerMatchCandidates(String customerSpace, long uploadPid, String tableName);

    void updateUploadConfig(String customerSpace, Long uploadPid, UploadConfig uploadConfig);

    void updateUploadStatus(String customerSpace, Long uploadPid, Upload.Status status);

    UploadStatsContainer appendStatistics(Long uploadPid, UploadStatsContainer container);

    void updateStatsWorkflowPid(Long uploadPid, Long statsTimestamp, Long workflowPid);

    void updateStatistics(Long uploadPid, Long statsTimestamp, UploadStats uploadStats);

    Upload setLatestStatistics(Long uploadPid, Long statsTimestamp);
}
