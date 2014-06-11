package com.latticeengines.dataplatform.entitymanager;

import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

public interface ModelDefinitionEntityMgr extends BaseEntityMgr<ModelDefinition> {

    ModelDefinition findByName(String name);

}
