package com.latticeengines.dataplatform.entitymanager;

import com.latticeengines.domain.exposed.dataplatform.Model;

public interface ModelEntityMgr extends BaseEntityMgr<Model> {

    Model findByObjectId(String id);

}
