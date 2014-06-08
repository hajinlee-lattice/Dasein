package com.latticeengines.dataplatform.dao;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

public interface ModelCommandDao extends BaseDao<ModelCommand> {
 
    List<ModelCommand> getNewAndInProgress();
}
