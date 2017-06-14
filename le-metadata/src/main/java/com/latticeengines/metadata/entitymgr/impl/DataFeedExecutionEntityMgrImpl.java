package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedImport;
import com.latticeengines.metadata.dao.DataFeedExecutionDao;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedImportEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("datafeedExecutionEntityMgr")
public class DataFeedExecutionEntityMgrImpl extends BaseEntityMgrImpl<DataFeedExecution>
        implements DataFeedExecutionEntityMgr {

    @Autowired
    private DataFeedExecutionDao datafeedExecutionDao;

    @Autowired
    private DataFeedImportEntityMgr datafeedImportEntityMgr;

    @Override
    public BaseDao<DataFeedExecution> getDao() {
        return datafeedExecutionDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DataFeedExecution execution) {
        super.create(execution);
        for (DataFeedImport datafeedImport : execution.getImports()) {
            datafeedImportEntityMgr.create(datafeedImport);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findByExecutionId(long executionId) {
        DataFeedExecution execution = findByField("pid", executionId);
        inflateDataFeedImport(execution);
        return execution;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findConsolidatingExecution(DataFeed datafeed) {
        DataFeedExecution execution = datafeedExecutionDao.findConsolidatingExecution(datafeed);
        inflateDataFeedImport(execution);
        return execution;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<DataFeedExecution> findByDataFeed(DataFeed datafeed) {
        List<DataFeedExecution> executions = datafeedExecutionDao.findByDataFeed(datafeed);
        for (DataFeedExecution execution : executions) {
            inflateDataFeedImport(execution);
        }
        return executions;
    }

    private void inflateDataFeedImport(DataFeedExecution execution) {
        if (!execution.getImports().isEmpty()) {
            for (DataFeedImport datafeedImport : execution.getImports()) {
                TableEntityMgr.inflateTable(datafeedImport.getDataTable());
            }
        }
    }
}
