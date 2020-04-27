package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.Upload;

public interface UploadEntityMgr extends BaseEntityMgrRepository<Upload, Long> {

    List<Upload> findBySourceId(String sourceId);

    List<Upload> findBySourceIdAndStatus(String sourceId, Upload.Status status);

    Upload findByUploadId(String uploadId);

    String findMatchResultTableNameByUploadId(String uploadId);

    String findMatchCandidatesTableNameByUploadId(String uploadId);

}
