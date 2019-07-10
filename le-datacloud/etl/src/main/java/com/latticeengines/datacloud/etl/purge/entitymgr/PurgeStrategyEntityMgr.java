package com.latticeengines.datacloud.etl.purge.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

public interface PurgeStrategyEntityMgr {
    List<PurgeStrategy> findAll();

    List<PurgeStrategy> findStrategiesByType(SourceType sourceType);

    PurgeStrategy findStrategyBySource(String source);

    PurgeStrategy findStrategyBySourceAndType(String source, SourceType sourceType);

    void insertAll(List<PurgeStrategy> strategies);

    void delete(PurgeStrategy strategy);
}
