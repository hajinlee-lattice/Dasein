package com.latticeengines.proxy.exposed.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;

public interface UploadProxy {
    Upload createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig);

    List<Upload> getUploads(String customerSpace, String sourceId, Upload.Status status);

    void updateConfig(String customerSpace, Long uploadId, UploadConfig uploadConfig);

    void updateStatus(String customerSpace, Long uploadId, Upload.Status status);


}
