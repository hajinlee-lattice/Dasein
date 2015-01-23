package com.latticeengines.dataplatform.dao.modeling;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modeling.Model;

public interface ModelDao extends BaseDao<Model> {

    Model findByObjectId(String id);
}
