package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.Upload;

public interface UploadEntityMgr extends BaseEntityMgrRepository<Upload, Long> {

    List<Upload> findBySourceId(String sourceId, Pageable pageable);

    Long countBySourceId(String sourceId);

    List<Upload> findBySourceIdAndStatus(String sourceId, Upload.Status status, Pageable pageable);

    Long countBySourceIdAndStatus(String sourceId, Upload.Status status);

    Upload findByUploadId(String uploadId);

    String findMatchResultTableNameByUploadId(String uploadId);

}
