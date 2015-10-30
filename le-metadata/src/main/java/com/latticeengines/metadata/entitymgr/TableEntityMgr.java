package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.Table;

public interface TableEntityMgr extends BaseEntityMgr<Table> {

    List<Table> getAll();

    Table findByName(String name);
    
}
