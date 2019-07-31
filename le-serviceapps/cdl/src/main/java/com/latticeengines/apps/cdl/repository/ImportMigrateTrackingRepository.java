package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;

public interface ImportMigrateTrackingRepository extends BaseJpaRepository<ImportMigrateTracking, Long> {

    ImportMigrateTracking findByPid(Long pid);
}
