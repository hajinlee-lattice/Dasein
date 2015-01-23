package com.latticeengines.dataplatform.entitymanager.modeling;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modeling.Model;

public interface ModelEntityMgr extends BaseEntityMgr<Model> {

    Model findByObjectId(String id);

}
