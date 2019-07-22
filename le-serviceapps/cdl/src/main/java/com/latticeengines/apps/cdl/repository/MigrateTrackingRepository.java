package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;

public interface MigrateTrackingRepository extends BaseJpaRepository<MigrateTracking, Long> {

    MigrateTracking findByPid(Long pid);
}
