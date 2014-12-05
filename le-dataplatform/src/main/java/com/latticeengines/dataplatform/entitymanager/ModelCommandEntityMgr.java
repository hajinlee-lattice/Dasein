package com.latticeengines.dataplatform.entitymanager;

import java.util.List;

import com.latticeengines.dataplatform.exposed.entitymanager.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

public interface ModelCommandEntityMgr extends BaseEntityMgr<ModelCommand> {

    List<ModelCommand> getNewAndInProgress();
}
