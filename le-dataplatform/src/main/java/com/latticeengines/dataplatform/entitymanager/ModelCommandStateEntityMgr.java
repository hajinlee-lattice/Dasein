package com.latticeengines.dataplatform.entitymanager;

import java.util.List;

import com.latticeengines.dataplatform.exposed.entitymanager.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public interface ModelCommandStateEntityMgr extends BaseEntityMgr<ModelCommandState> {

    List<ModelCommandState> findByModelCommandAndStep(ModelCommand modelCommand, ModelCommandStep modelCommandStep);
}
