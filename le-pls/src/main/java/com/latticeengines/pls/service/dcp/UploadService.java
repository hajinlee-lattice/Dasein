package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;

public interface UploadService {

    List<Upload> getAllBySourceId(String sourceId, Upload.Status status);

    Upload getByUploadId(long uploadPid);

    String generateToken(String uploadId);

    void sendUploadEmail(UploadEmailInfo uploadEmailInfo);
}
