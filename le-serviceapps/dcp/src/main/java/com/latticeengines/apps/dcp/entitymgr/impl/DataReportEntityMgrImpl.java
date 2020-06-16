package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.Date;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.DataReportDao;
import com.latticeengines.apps.dcp.entitymgr.DataReportEntityMgr;
import com.latticeengines.apps.dcp.repository.DataReportRepository;
import com.latticeengines.apps.dcp.repository.reader.DataReportReaderRepository;
import com.latticeengines.apps.dcp.repository.writer.DataReportWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

@Component("dataReportEntityMgr")
public class DataReportEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<DataReportRepository, DataReportRecord, Long>
        implements DataReportEntityMgr {

    @Inject
    private DataReportEntityMgrImpl _self;

    @Inject
    private DataReportDao dataReportDao;

    @Inject
    private DataReportReaderRepository dataReportReaderRepository;

    @Inject
    private DataReportWriterRepository dataReportWriterRepository;

    @Override
    protected DataReportRepository getReaderRepo() {
        return dataReportReaderRepository;
    }

    @Override
    protected DataReportRepository getWriterRepo() {
        return dataReportWriterRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<DataReportRepository, DataReportRecord, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<DataReportRecord> getDao() {
        return dataReportDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataReportRecord findDataReportRecord(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().findByLevelAndOwnerId(level, ownerId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataReport.BasicStats findDataReportBasicStats(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().findBasicStatsByLevelAndOwnerId(level, ownerId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean existsDataReport(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().existsByLevelAndOwnerId(level, ownerId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long findDataReportPid(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().findPidByLevelAndOwnerId(level, ownerId);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecord(Long pid, DataReport.BasicStats basicStats) {
        dataReportWriterRepository.updateDataReport(pid, new Date(), basicStats);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecord(Long pid, DataReport.InputPresenceReport inputPresenceReport) {
        dataReportWriterRepository.updateDataReport(pid, new Date(), inputPresenceReport);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecord(Long pid, DataReport.GeoDistributionReport geoDistributionReport) {
        dataReportWriterRepository.updateDataReport(pid, new Date(), geoDistributionReport);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecord(Long pid, DataReport.MatchToDUNSReport matchToDUNSReport) {
        dataReportWriterRepository.updateDataReport(pid, new Date(), matchToDUNSReport);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecord(Long pid, DataReport.DuplicationReport duplicationReport) {
        dataReportWriterRepository.updateDataReport(pid, new Date(), duplicationReport);
    }
}
