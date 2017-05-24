package com.latticeengines.dantedb.exposed.dao.impl;

import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public abstract class BaseDanteDaoImpl<T extends HasPid> extends BaseDaoImpl<T>// BaseDaoWithAssignedSessionFactoryImpl<T>
        implements BaseDanteDao<T> {

    public T findByExternalID(String externalID) {
        return findByField("External_ID", externalID);
    }
}
