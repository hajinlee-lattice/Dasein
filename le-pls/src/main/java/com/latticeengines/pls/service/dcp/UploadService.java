package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Upload;

public interface UploadService {

    List<Upload> getAllBySourceId(String sourceId, Upload.Status status);

    Upload getByUploadId(long uploadPid);

}
