package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.metadata.Table;

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
    public List<Object[]> findPidAndDunsCountTableName(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().findPidAndDunsCountTableName(level, ownerId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataReport.BasicStats findDataReportBasicStats(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().findBasicStatsByLevelAndOwnerId(level, ownerId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Map<String, DataReport.BasicStats> findDataReportBasicStatsByLevel(DataReportRecord.Level level) {
        List<Object[]> result = getReadOrWriteRepository().findBasicStatsByLevel(level);
        return convertBasicStats(result);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Map<String, DataReport.BasicStats> findBasicStatsByParentLevelAndOwnerId(DataReportRecord.Level parentLevel, String parentOwnerId) {
        List<Object[]> result = getReadOrWriteRepository().findBasicStatsByParentLevelAndOwnerId(parentLevel,
                parentOwnerId);
        return convertBasicStats(result);
    }

    private Map<String, DataReport.BasicStats> convertBasicStats(List<Object[]> rawList) {
        if (CollectionUtils.isEmpty(rawList)) {
            return Collections.emptyMap();
        }
        Map<String, DataReport.BasicStats> basicStatsMap = new HashMap<>();
        rawList.forEach(columns -> basicStatsMap.put((String) columns[0], (DataReport.BasicStats) columns[1]));
        return basicStatsMap;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean existsDataReport(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().existsByLevelAndOwnerId(level, ownerId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int countSiblingsByParentLevelAndOwnerId(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().countSiblingsByParentLevelAndOwnerId(level, ownerId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Set<String> findChildrenIds(DataReportRecord.Level level, String ownerId, boolean readyForRollup) {
        return getReadOrWriteRepository().findChildrenIdsByParentLevelAndOwnerId(level, ownerId, readyForRollup);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long findDataReportPid(DataReportRecord.Level level, String ownerId) {
        return getReadOrWriteRepository().findPidByLevelAndOwnerId(level, ownerId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long findParentId(Long pid) {
        return getReadOrWriteRepository().findParentIdByPid(pid);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateReadyForRollup(Long pid) {
        dataReportWriterRepository.updateReadyForRollup(pid, new Date());
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateReadyForRollupIfNotReady(Long pid) {
        dataReportWriterRepository.updateReadyForRollupIfNotReady(pid, new Date());
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecord(Long pid, DataReport.BasicStats basicStats) {
        dataReportWriterRepository.updateDataReport(pid, new Date(), basicStats);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void uploadDataReportRecord(Long pid, Table dunsCountTable, Date snapShotTime) {
        dataReportWriterRepository.updateDataReport(pid, new Date(), snapShotTime,
                dunsCountTable);
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

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecordIfNull(Long pid, DataReport.BasicStats basicStats) {
        dataReportWriterRepository.updateDataReportIfNull(pid, new Date(), basicStats);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecordIfNull(Long pid, DataReport.InputPresenceReport inputPresenceReport) {
        dataReportWriterRepository.updateDataReportIfNull(pid, new Date(), inputPresenceReport);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecordIfNull(Long pid, DataReport.GeoDistributionReport geoDistributionReport) {
        dataReportWriterRepository.updateDataReportIfNull(pid, new Date(), geoDistributionReport);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecordIfNull(Long pid, DataReport.MatchToDUNSReport matchToDUNSReport) {
        dataReportWriterRepository.updateDataReportIfNull(pid, new Date(), matchToDUNSReport);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateDataReportRecordIfNull(Long pid, DataReport.DuplicationReport duplicationReport) {
        dataReportWriterRepository.updateDataReportIfNull(pid, new Date(), duplicationReport);
    }
}
