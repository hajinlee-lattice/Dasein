package com.latticeengines.proxy.exposed.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;

public interface UploadProxy {

    UploadDetails createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig);

    List<UploadDetails> getUploads(String customerSpace, String sourceId, Upload.Status status);

    UploadDetails getUploadByUploadId(String customerSpace, String uploadId);

    void registerMatchResult(String customerSpace, String uploadId, String tableName);

    void updateUploadConfig(String customerSpace, String uploadId, UploadConfig uploadConfig);

    void updateUploadStatus(String customerSpace, String uploadId, Upload.Status status);

    void updateStatsContent(String customerSpace, String uploadId, long statsPid, UploadStats uploadStats);

    void setLatestStats(String customerSpace, String uploadId, long statsPid);

}
