package com.latticeengines.dataplatform.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.metrics.YarnMetricGeneratorInfo;

public interface YarnMetricGeneratorInfoEntityMgr extends BaseEntityMgr<YarnMetricGeneratorInfo> {

    YarnMetricGeneratorInfo findByName(String name);

}
