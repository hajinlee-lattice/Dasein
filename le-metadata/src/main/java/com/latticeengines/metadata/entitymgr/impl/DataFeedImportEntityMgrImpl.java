package com.latticeengines.metadata.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.metadata.dao.DataFeedImportDao;
import com.latticeengines.metadata.datafeed.repository.DataFeedImportRepository;
import com.latticeengines.metadata.entitymgr.DataFeedImportEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("datafeedImportEntityMgr")
public class DataFeedImportEntityMgrImpl extends BaseEntityMgrRepositoryImpl<DataFeedImport, Long>
        implements DataFeedImportEntityMgr {

    @Inject
    private DataFeedImportRepository dataFeedImportRepository;

    @Inject
    private DataFeedImportDao datafeedImportDao;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Override
    public BaseJpaRepository<DataFeedImport, Long> getRepository() {
        return dataFeedImportRepository;
    }

    @Override
    public BaseDao<DataFeedImport> getDao() {
        return datafeedImportDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(DataFeedImport dataFeedImport) {
        if (dataFeedImport.getDataTable() != null && dataFeedImport.getDataTable().getPid() == null) {
            dataFeedImport.getDataTable().setTableType(TableType.DATATABLE);
            tableEntityMgr.create(dataFeedImport.getDataTable());
        }
        datafeedImportDao.create(dataFeedImport);
    }

}
