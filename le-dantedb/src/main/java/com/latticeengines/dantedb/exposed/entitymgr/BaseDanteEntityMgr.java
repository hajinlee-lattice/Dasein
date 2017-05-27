package com.latticeengines.dantedb.exposed.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.dante.HasDanteAuditingFields;

public interface BaseDanteEntityMgr<T extends HasDanteAuditingFields> extends BaseEntityMgr<T> {
}
