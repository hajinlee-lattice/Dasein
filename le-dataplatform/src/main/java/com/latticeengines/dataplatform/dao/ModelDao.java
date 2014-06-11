package com.latticeengines.dataplatform.dao;

import com.latticeengines.domain.exposed.dataplatform.Model;

public interface ModelDao extends BaseDao<Model> {

    Model findByObjectId(String id);
}
