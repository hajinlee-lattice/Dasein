package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.Sampling;

public interface SamplingEntityMgr extends BaseEntityMgr<Sampling> {

    void createSamplings(List<Sampling> samplings);

}
