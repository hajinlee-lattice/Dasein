package com.latticeengines.dataplatform.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;

public interface ModelCommandResultDao extends BaseDao<ModelCommandResult> {

    ModelCommandResult findByModelCommand(ModelCommand modelCommand);

}
