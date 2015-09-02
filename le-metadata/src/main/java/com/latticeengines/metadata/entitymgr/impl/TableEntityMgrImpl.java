package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.dao.TableDao;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("tableEntityMgr")
public class TableEntityMgrImpl extends BaseEntityMgrImpl<Table> implements TableEntityMgr {
    
    @Autowired
    private TableDao tableDao;

    @Override
    public BaseDao<Table> getDao() {
        return tableDao;
    }

}
