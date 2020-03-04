package com.latticeengines.apps.dcp.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.Upload;

public interface UploadRepository extends BaseJpaRepository<Upload, Long> {

    List<Upload> findBySourceId(String sourceId);

    List<Upload> findBySourceIdAndStatus(String sourceId, Upload.Status status);
}
