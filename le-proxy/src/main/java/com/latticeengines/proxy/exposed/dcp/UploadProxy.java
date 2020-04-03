package com.latticeengines.proxy.exposed.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadStats;

public interface UploadProxy {

    Upload createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig);

    List<Upload> getUploads(String customerSpace, String sourceId, Upload.Status status);

    Upload getUpload(String customerSpace, Long uploadPid);

    void registerMatchResult(String customerSpace, long uploadPid, String tableName);

    void updateUploadConfig(String customerSpace, Long uploadPid, UploadConfig uploadConfig);

//    void updateUploadStats(String customerSpace, Long uploadPid, UploadStats uploadStats);

    void updateUploadStatus(String customerSpace, Long uploadPid, Upload.Status status);

    void updateStatsContent(String customerSpace, long uploadPid, long statsPid, UploadStats uploadStats);

    void setLatestStats(String customerSpace, long uploadPid, long statsPid);

}
