package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;

public interface UploadService {

    List<Upload> getUploads(String customerSpace, String sourceId);

    List<Upload> getUploads(String customerSpace, String sourceId, Upload.Status status);
}
