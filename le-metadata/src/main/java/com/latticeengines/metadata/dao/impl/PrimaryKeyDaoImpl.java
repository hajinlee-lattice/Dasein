package com.latticeengines.metadata.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.metadata.dao.PrimaryKeyDao;

public class PrimaryKeyDaoImpl extends BaseDaoImpl<PrimaryKey> implements PrimaryKeyDao {

    @Override
    protected Class<PrimaryKey> getEntityClass() {
        return PrimaryKey.class;
    }

}
