package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;

@Component("lastModifiedKeyDao")
public class LastModifiedKeyDaoImpl extends BaseDaoImpl<LastModifiedKey> implements LastModifiedKeyDao {

    @Override
    protected Class<LastModifiedKey> getEntityClass() {
        return LastModifiedKey.class;
    }

}
