package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface UploadService {

    List<Upload> getAllBySourceId(String sourceId, Upload.Status status);

    Upload getByUploadId(long uploadPid);

    void downloadUpload(String uploadId, HttpServletRequest request, HttpServletResponse response) throws Exception;
}
