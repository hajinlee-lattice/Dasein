package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;

public interface AtlasSchedulingEntityMgr extends BaseEntityMgrRepository<AtlasScheduling, Long> {

    void createSchedulingObj(AtlasScheduling atlasScheduling);

    void updateSchedulingObj(AtlasScheduling atlasScheduling);

    AtlasScheduling findAtlasSchedulingByType(AtlasScheduling.ScheduleType type);

    List<AtlasScheduling> getAllAtlasSchedulingByType(AtlasScheduling.ScheduleType type);

}
