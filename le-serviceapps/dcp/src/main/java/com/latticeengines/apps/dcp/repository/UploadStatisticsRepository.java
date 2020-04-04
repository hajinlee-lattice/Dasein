package com.latticeengines.apps.dcp.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;

public interface UploadStatisticsRepository extends BaseJpaRepository<UploadStatsContainer, Long> {

    UploadStatsContainer findByUploadAndPid(Upload upload, Long pid);

    UploadStatsContainer findByUploadAndIsLatest(Upload upload, Boolean isLatest);

}
