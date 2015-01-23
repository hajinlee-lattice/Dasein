package com.latticeengines.dataplatform.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;

public interface ModelCommandLogDao extends BaseDao<ModelCommandLog> {

    List<ModelCommandLog> findByModelCommand(ModelCommand modelCommand);

}
