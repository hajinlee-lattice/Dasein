package com.latticeengines.dataplatform.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;

public interface ModelCommandLogEntityMgr extends BaseEntityMgr<ModelCommandLog> {

    List<ModelCommandLog> findByModelCommand(ModelCommand modelCommand);
}
