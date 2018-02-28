package com.latticeengines.datacloud.etl.purge.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

public interface PurgeStrategyEntityMgr {
    List<PurgeStrategy> findStrategiesByType(SourceType sourceType);

    void insertAll(List<PurgeStrategy> strategies);

    void deleteAll();
}
