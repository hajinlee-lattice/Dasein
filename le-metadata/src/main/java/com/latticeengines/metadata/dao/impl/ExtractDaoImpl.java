package com.latticeengines.metadata.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.metadata.dao.ExtractDao;

public class ExtractDaoImpl extends BaseDaoImpl<Extract> implements ExtractDao {

    @Override
    protected Class<Extract> getEntityClass() {
        return Extract.class;
    }

}
