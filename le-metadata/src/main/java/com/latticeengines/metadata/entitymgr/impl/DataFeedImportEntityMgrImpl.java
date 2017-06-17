package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.dao.DataFeedImportDao;
import com.latticeengines.metadata.entitymgr.DataFeedImportEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("datafeedImportEntityMgr")
public class DataFeedImportEntityMgrImpl extends BaseEntityMgrImpl<DataFeedImport> implements DataFeedImportEntityMgr {

    @Autowired
    private DataFeedImportDao datafeedImportDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Override
    public BaseDao<DataFeedImport> getDao() {
        return datafeedImportDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DataFeedImport dataFeedImport) {
        if (dataFeedImport.getDataTable() != null && dataFeedImport.getDataTable().getPid() == null) {
            dataFeedImport.getDataTable().setTableType(TableType.DATATABLE);
            tableEntityMgr.create(dataFeedImport.getDataTable());
        }
        super.create(dataFeedImport);
    }

}
