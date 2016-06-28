package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.Algorithm;

public interface AlgorithmEntityMgr extends BaseEntityMgr<Algorithm> {

    void createAlgorithms(List<Algorithm> algorithms);

}
