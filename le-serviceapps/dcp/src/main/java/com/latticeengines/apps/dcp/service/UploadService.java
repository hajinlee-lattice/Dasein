package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;

public interface UploadService {

    List<Upload> getUploads(String customerSpace, String sourceId);

    List<Upload> getUploads(String customerSpace, String sourceId, Upload.Status status);

    Upload createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig);

    void updateUploadConfig(String customerSpace, Long uploadPid, UploadConfig uploadConfig);

    void updateUploadStatus(String customerSpace, Long uploadPid, Upload.Status status);
}
