package com.latticeengines.dataplatform.dao;

import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

public interface ModelDefinitionDao extends BaseDao<ModelDefinition> {

    ModelDefinition findByName(String name);
}
