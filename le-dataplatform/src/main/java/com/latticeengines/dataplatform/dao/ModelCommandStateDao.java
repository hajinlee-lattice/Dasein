package com.latticeengines.dataplatform.dao;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public interface ModelCommandStateDao extends BaseDao<ModelCommandState> {

    List<ModelCommandState> findByModelCommandAndStep(ModelCommand modelCommand, ModelCommandStep modelCommandStep);
}
