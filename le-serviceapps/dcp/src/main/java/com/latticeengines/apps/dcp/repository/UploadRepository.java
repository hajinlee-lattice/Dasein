package com.latticeengines.apps.dcp.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.Upload;

public interface UploadRepository extends BaseJpaRepository<Upload, Long> {

    List<Upload> findBySourceId(String sourceId, Pageable pageable);

    List<Upload> findAll();

    Long countBySourceId(String sourceId);

    List<Upload> findBySourceIdAndStatus(String sourceId, Upload.Status status, Pageable pageable);

    Long countBySourceIdAndStatus(String sourceId, Upload.Status status);

    Upload findByUploadId(String uploadId);

    @Query("select t.name from Upload as u join u.matchResult as t where u.uploadId = ?1")
    String findMatchResultTableNameByUploadId(String uploadId);

}
