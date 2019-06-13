package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataFeedExecutionDao;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedImportEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.DataFeedExecutionRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
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
        for (DataFeedImport datafeedImport : execution.getImports()) {
            datafeedImportEntityMgr.createOrUpdate(datafeedImport);
        }
        super.update(execution);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findByPid(Long executionId) {
        if (executionId == null) {
            return null;
        }
        DataFeedExecution execution = dataFeedExecutionRepository.findById(executionId).orElse(null);
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
    public List<DataFeedExecution> findActiveExecutionByDataFeedAndJobType(DataFeed dataFeed, DataFeedExecutionJobType jobType) {
        return datafeedExecutionDao.findAllByFields("FK_FEED_ID", dataFeed.getPid(), "JOB_TYPE", jobType, "STATUS",
                DataFeedExecution.Status.Started);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findFirstByDataFeedAndJobTypeOrderByPidDesc(DataFeed datafeed,
            DataFeedExecutionJobType jobType) {
        DataFeedExecution execution = dataFeedExecutionRepository.findFirstByDataFeedAndJobTypeOrderByPidDesc(datafeed,
                jobType);
        inflateDataFeedImport(execution);
        return execution;
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

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public DataFeedExecution findByStatusAndWorkflowId(DataFeedExecution.Status status, Long workflowId) {
        if (status == null || workflowId == null) {
            return null;
        }
        DataFeedExecution execution = dataFeedExecutionRepository.findByStatusAndWorkflowId(status, workflowId);
        inflateDataFeedImport(execution);
        return execution;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public DataFeedExecution updateStatus(DataFeedExecution execution) {
        if (execution == null) {
            return null;
        }

        return datafeedExecutionDao.updateStatus(execution);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public DataFeedExecution updateRetryCount(DataFeedExecution execution) {
        if (execution == null) {
            return null;
        }
        return datafeedExecutionDao.updateRetryCount(execution);
    }
}
