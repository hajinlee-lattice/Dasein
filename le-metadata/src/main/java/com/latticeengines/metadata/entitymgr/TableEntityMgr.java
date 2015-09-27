package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.Table;

public interface TableEntityMgr extends BaseEntityMgr<Table> {

    public void delete(Table table);
}
