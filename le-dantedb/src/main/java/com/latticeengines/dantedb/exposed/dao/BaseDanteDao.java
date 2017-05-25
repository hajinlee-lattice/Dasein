package com.latticeengines.dantedb.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dantetalkingpoints.HasDanteAuditingFields;

public interface BaseDanteDao<T extends HasDanteAuditingFields> extends BaseDao<T> {
    T findByExternalID(String externalID);
}
