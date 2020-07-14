package com.latticeengines.pls.service.dcp;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.dcp.UploadFileDownloadConfig;

public interface UploadService {

    List<UploadDetails> getAllBySourceId(String sourceId, Upload.Status status, Boolean includeConfig);

    UploadDetails getByUploadId(String uploadId, Boolean includeConfig);

    String generateToken(String uploadId, List<UploadFileDownloadConfig.FileType> files);

    void sendUploadEmail(UploadEmailInfo uploadEmailInfo);

    UploadDetails startImport(DCPImportRequest importRequest);

    ApplicationId submitImportRequest(DCPImportRequest importRequest);
}
