package com.latticeengines.modelquality.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.Sampling;

public interface SamplingEntityMgr extends BaseEntityMgr<Sampling> {

    Sampling findByName(String samplingConfigName);

    Sampling getLatestProductionVersion();
}
