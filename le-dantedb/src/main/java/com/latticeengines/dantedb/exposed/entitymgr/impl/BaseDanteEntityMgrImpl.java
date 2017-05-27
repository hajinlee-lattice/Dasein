package com.latticeengines.dantedb.exposed.entitymgr.impl;

import com.latticeengines.dantedb.exposed.entitymgr.BaseDanteEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.HasDanteAuditingFields;

public abstract class BaseDanteEntityMgrImpl<T extends HasDanteAuditingFields> extends BaseEntityMgrImpl<T>
        implements BaseDanteEntityMgr<T> {
}
