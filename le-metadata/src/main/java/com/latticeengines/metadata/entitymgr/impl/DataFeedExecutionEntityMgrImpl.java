package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.metadata.dao.DataFeedExecutionDao;
import com.latticeengines.metadata.datafeed.repository.DataFeedExecutionRepository;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedImportEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("datafeedExecutionEntityMgr")
public class DataFeedExecutionEntityMgrImpl extends BaseEntityMgrRepositoryImpl<DataFeedExecution, Long>
        implements DataFeedExecutionEntityMgr {

    @Inject
    private DataFeedExecutionRepository dataFeedExecutionRepository;

    @Inject
    private DataFeedExecutionDao datafeedExecutionDao;

    @Inject
    private DataFeedImportEntityMgr datafeedImportEntityMgr;

    @Override
    public BaseDao<DataFeedExecution> getDao() {
        return datafeedExecutionDao;
    }

    @Override
    public BaseJpaRepository<DataFeedExecution, Long> getRepository() {
        return dataFeedExecutionRepository;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(DataFeedExecution execution) {
        super.create(execution);
        for (DataFeedImport datafeedImport : execution.getImports()) {
            datafeedImportEntityMgr.create(datafeedImport);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateImports(DataFeedExecution execution) {
        super.update(execution);
        for (DataFeedImport datafeedImport : execution.getImports()) {
            datafeedImportEntityMgr.create(datafeedImport);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findByPid(Long executionId) {
        if (executionId == null) {
            return null;
        }
        DataFeedExecution execution = dataFeedExecutionRepository.findByPid(executionId);
        inflateDataFeedImport(execution);
        return execution;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public List<DataFeedExecution> findByDataFeed(DataFeed datafeed) {
        List<DataFeedExecution> executions = dataFeedExecutionRepository.findByDataFeed(datafeed);
        for (DataFeedExecution execution : executions) {
            inflateDataFeedImport(execution);
        }
        return executions;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findFirstByDataFeedAndJobTypeOrderByPidDesc(DataFeed datafeed,
            DataFeedExecutionJobType jobType) {
        return dataFeedExecutionRepository.findFirstByDataFeedAndJobTypeOrderByPidDesc(datafeed, jobType);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public int countByDataFeedAndJobType(DataFeed datafeed, DataFeedExecutionJobType jobType) {
        return dataFeedExecutionRepository.countByDataFeedAndJobType(datafeed, jobType);
    }

    private void inflateDataFeedImport(DataFeedExecution execution) {
        if (execution != null && !execution.getImports().isEmpty()) {
            for (DataFeedImport datafeedImport : execution.getImports()) {
                TableEntityMgr.inflateTable(datafeedImport.getDataTable());
            }
        }
    }
}
