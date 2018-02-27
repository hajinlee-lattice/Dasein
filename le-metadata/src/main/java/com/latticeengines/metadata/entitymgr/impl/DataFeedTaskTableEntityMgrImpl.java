package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;
import com.latticeengines.metadata.dao.DataFeedTaskTableDao;
import com.latticeengines.metadata.datafeed.repository.DataFeedTaskTableRepository;
import com.latticeengines.metadata.entitymgr.DataFeedTaskTableEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("datafeedTaskTableEntityMgr")
public class DataFeedTaskTableEntityMgrImpl extends BaseEntityMgrRepositoryImpl<DataFeedTaskTable, Long>
        implements DataFeedTaskTableEntityMgr {

    @Inject
    private DataFeedTaskTableRepository datafeedTaskTableRepository;

    @Inject
    private DataFeedTaskTableDao datafeedTaskTableDao;

    @Override
    public BaseJpaRepository<DataFeedTaskTable, Long> getRepository() {
        return datafeedTaskTableRepository;
    }

    @Override
    public BaseDao<DataFeedTaskTable> getDao() {
        return datafeedTaskTableDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public Table peekFirstDataTable(DataFeedTask datafeedTask) {
        Table table = datafeedTaskTableRepository.findFirstByDataFeedTaskOrderByPidAsc(datafeedTask).getTable();
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public Table pollFirstDataTable(DataFeedTask datafeedTask) {
        DataFeedTaskTable dataFeedTaskTable = datafeedTaskTableRepository
                .findFirstByDataFeedTaskOrderByPidAsc(datafeedTask);
        if (dataFeedTaskTable == null) {
            return null;
        }
        Table table = dataFeedTaskTable.getTable();
        if (table == null) {
            return null;
        }
        delete(dataFeedTaskTable);
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public int countDataFeedTaskTables(DataFeedTask datafeedTask) {
        return datafeedTaskTableRepository.countByDataFeedTask(datafeedTask);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<DataFeedTaskTable> getDataFeedTaskTables(DataFeedTask datafeedTask) {
        return datafeedTaskTableRepository.findByDataFeedTask(datafeedTask);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<DataFeedTaskTable> getInflatedDataFeedTaskTables(DataFeedTask datafeedTask) {
        List<DataFeedTaskTable> datafeedTaskTables = datafeedTaskTableRepository.findByDataFeedTask(datafeedTask);
        datafeedTaskTables.stream().map(DataFeedTaskTable::getTable).forEach(t -> {
            TableEntityMgr.inflateTable(t);
        });
        return datafeedTaskTables;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void deleteDataFeedTaskTables(DataFeedTask dataFeedTask) {
        datafeedTaskTableDao.deleteDataFeedTaskTables(dataFeedTask);
    }

}
