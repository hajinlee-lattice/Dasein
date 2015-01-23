package com.latticeengines.dataplatform.dao.modeling;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;

public interface ModelDefinitionDao extends BaseDao<ModelDefinition> {

    ModelDefinition findByName(String name);
}
