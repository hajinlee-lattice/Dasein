package com.latticeengines.proxy.exposed.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;

public interface UploadProxy {

    Upload createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig);

    List<Upload> getUploads(String customerSpace, String sourceId, Upload.Status status);

    Upload getUpload(String customerSpace, Long uploadPid);

    void updateUploadConfig(String customerSpace, Long uploadPid, UploadConfig uploadConfig);

    void updateUploadStatus(String customerSpace, Long uploadPid, Upload.Status status);

}
