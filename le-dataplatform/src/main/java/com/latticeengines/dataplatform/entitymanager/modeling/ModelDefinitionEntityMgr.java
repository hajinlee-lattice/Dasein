package com.latticeengines.dataplatform.entitymanager.modeling;

import com.latticeengines.dataplatform.entitymanager.BaseEntityMgr;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;

public interface ModelDefinitionEntityMgr extends BaseEntityMgr<ModelDefinition> {

    ModelDefinition findByName(String name);

}
