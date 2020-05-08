package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;

public interface UploadService {

    List<UploadDetails> getAllBySourceId(String sourceId, Upload.Status status);

    UploadDetails getByUploadId(String uploadId);

    String generateToken(String uploadId);

    void sendUploadEmail(UploadEmailInfo uploadEmailInfo);

    UploadDetails startImport(String projectId, String sourceId, String sourceFileName);
}
