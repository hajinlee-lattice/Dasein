package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;

public interface AtlasSchedulingRepository extends BaseJpaRepository<AtlasScheduling, Long> {

    AtlasScheduling findByType(AtlasScheduling.ScheduleType type);

    List<AtlasScheduling> findAllByType(AtlasScheduling.ScheduleType type);
}
