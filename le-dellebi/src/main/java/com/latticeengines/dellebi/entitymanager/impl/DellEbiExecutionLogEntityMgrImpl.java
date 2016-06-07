package com.latticeengines.dellebi.entitymanager.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.dellebi.dao.DellEbiExecutionLogDao;
import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Service
public class DellEbiExecutionLogEntityMgrImpl extends BaseEntityMgrImpl<DellEbiExecutionLog>
        implements DellEbiExecutionLogEntityMgr {

    @Autowired
    DellEbiExecutionLogDao dellEbiExecutionLogDao;

    @Override
    @Transactional(value = "transactionManagerDellEbiCfg")
    public void executeUpdate(DellEbiExecutionLog dellEbiExecutionLog) {
        dellEbiExecutionLogDao.update(dellEbiExecutionLog);
    }

    @Override
    @Transactional(value = "transactionManagerDellEbiCfg")
    public void create(DellEbiExecutionLog dellEbiExecutionLog) {
        dellEbiExecutionLogDao.create(dellEbiExecutionLog);
    }

    @Override
    @Transactional(value = "transactionManagerDellEbiCfg")
    public void createOrUpdate(DellEbiExecutionLog dellEbiExecutionLog) {
        dellEbiExecutionLogDao.createOrUpdate(dellEbiExecutionLog);
    }

    @Override
    @Transactional(value = "transactionManagerDellEbiCfg")
    public DellEbiExecutionLog getEntryByFile(String file) {
        return dellEbiExecutionLogDao.getEntryByFile(file);
    }

    @Override
    @Transactional(value = "transactionManagerDellEbiCfg")
    public List<DellEbiExecutionLog> getEntriesByFile(String file) {
        return dellEbiExecutionLogDao.getEntriesByFile(file);
    }

    @Override
    public BaseDao<DellEbiExecutionLog> getDao() {
        return dellEbiExecutionLogDao;
    }

    @Override
    @Transactional(value = "transactionManagerDellEbiCfg")
    public void recordFailure(DellEbiExecutionLog dellEbiExecutionLog, String err) {
        if (dellEbiExecutionLog == null || err == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Failed.getStatus());
        dellEbiExecutionLog.setEndDate(new Date());
        dellEbiExecutionLog.setError(err);
        executeUpdate(dellEbiExecutionLog);
    }

    @Override
    @Transactional(value = "transactionManagerDellEbiCfg")
    public void recordRetryFailure(DellEbiExecutionLog dellEbiExecutionLog, String err) {
        if (dellEbiExecutionLog == null || err == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.TriedFailed.getStatus());
        dellEbiExecutionLog.setEndDate(new Date());
        dellEbiExecutionLog.setError(err);
        executeUpdate(dellEbiExecutionLog);
    }

}
