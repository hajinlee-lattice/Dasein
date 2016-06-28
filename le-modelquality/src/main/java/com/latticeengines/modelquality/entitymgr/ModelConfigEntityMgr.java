package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;

public interface ModelConfigEntityMgr extends BaseEntityMgr<ModelConfig> {

    void createModelConfigs(List<ModelConfig> modelconfigs);

}
