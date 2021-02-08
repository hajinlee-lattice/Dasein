package com.latticeengines.apps.dcp.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.DataReportEntityMgr;
import com.latticeengines.apps.dcp.service.DataReportService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.dataReport.DataReportRollupStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.metadata.service.MetadataService;

import avro.shaded.com.google.common.base.Preconditions;

@Service("dataReportService")
public class DataReportServiceImpl implements DataReportService {

    private static final Logger log = LoggerFactory.getLogger(DataReportServiceImpl.class);

    @Inject
    private DataReportEntityMgr dataReportEntityMgr;

    @Inject
    private ProjectService projectService;

    @Inject
    private UploadService uploadService;

    @Inject
    private MetadataService metadataService;

    @Override
    public DataReport getDataReport(String customerSpace, DataReportRecord.Level level, String ownerId) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        DataReportRecord dataReportRecord = dataReportEntityMgr.findDataReportRecord(level, ownerId);
        return convertRecordToDataReport(dataReportRecord);
    }

    @Override
    public DataReport.BasicStats getDataReportBasicStats(String customerSpace, DataReportRecord.Level level, String ownerId) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        return dataReportEntityMgr.findDataReportBasicStats(level, ownerId);
    }

    @Override
    public Map<String, DataReport.BasicStats> getDataReportBasicStats(String customerSpace, DataReportRecord.Level level) {
        return dataReportEntityMgr.findDataReportBasicStatsByLevel(level);
    }

    @Override
    public Map<String, DataReport.BasicStats> getDataReportBasicStatsByParent(String customerSpace, DataReportRecord.Level parentLevel, String parentOwnerId) {
        return dataReportEntityMgr.findBasicStatsByParentLevelAndOwnerId(parentLevel, parentOwnerId);
    }

    @Override
    public DataReportRecord getDataReportRecord(String customerSpace, DataReportRecord.Level level, String ownerId) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        return dataReportEntityMgr.findDataReportRecord(level, ownerId);
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport dataReport) {
        if (dataReport == null) {
            return;
        }
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            DataReportRecord dataReportRecord = dataReportEntityMgr.findDataReportRecord(level, ownerId);
            if (dataReport.getBasicStats() != null) {
                dataReportRecord.setBasicStats(dataReport.getBasicStats());
            }
            if (dataReport.getInputPresenceReport() != null) {
                dataReportRecord.setInputPresenceReport(dataReport.getInputPresenceReport());
            }
            if (dataReport.getGeoDistributionReport() != null) {
                dataReportRecord.setGeoDistributionReport(dataReport.getGeoDistributionReport());
            }
            if (dataReport.getMatchToDUNSReport() != null) {
                dataReportRecord.setMatchToDUNSReport(dataReport.getMatchToDUNSReport());
            }
            if (dataReport.getDuplicationReport() != null) {
                dataReportRecord.setDuplicationReport(dataReport.getDuplicationReport());
            }

            dataReportRecord.setRefreshTime(new Date());
            dataReportEntityMgr.update(dataReportRecord);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setBasicStats(dataReport.getBasicStats());
            dataReportRecord.setInputPresenceReport(dataReport.getInputPresenceReport());
            dataReportRecord.setGeoDistributionReport(dataReport.getGeoDistributionReport());
            dataReportRecord.setMatchToDUNSReport(dataReport.getMatchToDUNSReport());
            dataReportRecord.setDuplicationReport(dataReport.getDuplicationReport());
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
            pid = dataReportRecord.getPid();
        }
    }

    @Override
    public void registerDunsCount(String customerSpace, DataReportRecord.Level level, String ownerId,
                           DunsCountCache cache) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        List<Object[]> result = dataReportEntityMgr.findPidAndDunsCountTableName(level, ownerId);
        if (CollectionUtils.isEmpty(result)) {
            log.info("data report is not registered");
            return ;
        }
        Object[] objects = result.get(0);
        Long pid = objects[0] != null ? (Long) objects[0] : null;
        String oldTableName = objects[1] != null ? (String) objects[1] : null;
        Preconditions.checkNotNull(pid, String.format("data record should exist for %s and %s.", level, ownerId));
        Table table = metadataService.getTable(CustomerSpace.parse(customerSpace), cache.getDunsCountTableName());
        Preconditions.checkNotNull(table, String.format("table shouldn't be null for %s", cache.getDunsCountTableName()));
        dataReportEntityMgr.updateDataReportRecord(pid, table, cache.getSnapshotTimestamp());

        DataReport dataReport = getDataReport(customerSpace, level, ownerId);
        Long parentId = dataReportEntityMgr.findParentId(pid);
        String parentOwnerId = getParentOwnerId(customerSpace, level, ownerId);
        DataReportRecord.Level parentLevel = level.getParentLevel();
        while (parentId != null) {
            // link the same duns count as child if parent is null
            int count = dataReportEntityMgr.updateDataReportRecordIfNull(parentId, table, cache.getSnapshotTimestamp());
            log.info("Some info: ownerId {}, level {}, count {}", parentOwnerId, parentId, count);
            // stop updating the parental node
            if (count == 0) {
                log.info("Stop registering duns count for {}", parentId);
                break;
            }
            // this is to keep the consistency, after linking the duns count as child,
            // override parent's data report too
            log.info("Begin overriding the data report for ownerId {} and level {} with {} after updating duns count",
                    parentOwnerId, parentLevel, JsonUtils.serialize(dataReport));
            SleepUtils.sleep(150);
            updateDataReport(customerSpace, parentLevel, parentOwnerId, dataReport);
            parentOwnerId = getParentOwnerId(customerSpace, parentLevel, parentOwnerId);
            parentLevel = parentLevel.getParentLevel();
            parentId = dataReportEntityMgr.findParentId(parentId);
        }
        if (StringUtils.isNotBlank(oldTableName)) {
            int count = dataReportEntityMgr.countRecordsByDunsCount(oldTableName);
            log.info("the count reference by data report record for {} is {}", oldTableName, count);
            if (count == 0) {
                log.info("There was an old duns count table {}, going to be marked as temporary.", oldTableName);
                RetentionPolicy retentionPolicy = RetentionPolicyUtil.toRetentionPolicy(7, RetentionPolicyTimeUnit.DAY);
                metadataService.updateTableRetentionPolicy(CustomerSpace.parse(customerSpace), oldTableName,
                        retentionPolicy);
            }
        } else {
            log.info("There weren't duns count table previously");
        }
    }

    /**
     * in data report, the logic only retrieves the data report with readyForRollup = true
     */
    @Override
    public Set<String> getChildrenIds(String customerSpace, DataReportRecord.Level level, String ownerId) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        Set<String> subOwnerIds = dataReportEntityMgr.findChildrenIds(level, ownerId);
        if (subOwnerIds == null) {
            return new HashSet<>();
        } else {
            return subOwnerIds;
        }
    }

    @Override
    public DunsCountCache getDunsCount(String customerSpace, DataReportRecord.Level level, String ownerId) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        DataReportRecord record = dataReportEntityMgr.findDataReportRecord(level, ownerId);
        if (record == null) {
            return new DunsCountCache();
        }
        List<Object[]> result = dataReportEntityMgr.findPidAndDunsCountTableName(level, ownerId);
        if (CollectionUtils.isEmpty(result)) {
            log.info("data report is not registered");
            return new DunsCountCache();
        }
        Object[] objects = result.get(0);
        String dunsCountTableName = objects[1] != null ? (String) objects[1] : null;
        DunsCountCache cache = new DunsCountCache();
        cache.setSnapshotTimestamp(record.getDataSnapshotTime());
        cache.setDunsCountTableName(dunsCountTableName);

        return cache;
    }

    @Override
    public void updateReadyForRollup(String customerSpace, DataReportRecord.Level level, String ownerId) {

        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        Preconditions.checkNotNull(pid);
        log.info("update ready for rollup for {}", pid);
        dataReportEntityMgr.updateReadyForRollup(pid);

        // update readyForRollup for parental data report if it's not ready
        Long parentId = dataReportEntityMgr.findParentId(pid);
        while (parentId != null) {
            int count = dataReportEntityMgr.updateReadyForRollupIfNotReady(parentId);
            log.info("parentId: {}, count: {}", parentId, count);
            // stop updating value for parental node
            if (count == 0) {
                log.info("stop updating ready for rollup for {}", parentId);
                break;
            }
            parentId = dataReportEntityMgr.findParentId(parentId);
        }

    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.BasicStats basicStats) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, basicStats);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setBasicStats(basicStats);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
            pid = dataReportRecord.getPid();
        }
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.InputPresenceReport inputPresenceReport) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, inputPresenceReport);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setInputPresenceReport(inputPresenceReport);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
            pid = dataReportRecord.getPid();
        }
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.GeoDistributionReport geoDistributionReport) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, geoDistributionReport);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setGeoDistributionReport(geoDistributionReport);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
            pid = dataReportRecord.getPid();
        }
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.MatchToDUNSReport matchToDUNSReport) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, matchToDUNSReport);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setMatchToDUNSReport(matchToDUNSReport);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
            pid = dataReportRecord.getPid();
        }
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.DuplicationReport duplicationReport) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, duplicationReport);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setDuplicationReport(duplicationReport);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
            pid = dataReportRecord.getPid();
        }
    }

    @Override
    public void copyDataReportToParent(String customerSpace, DataReportRecord.Level level, String ownerId) {
        DataReport dataReport = getDataReport(customerSpace, level, ownerId);
        if (dataReport == null) {
            return;
        }
        if (DataReportRecord.Level.Tenant.equals(level)) {
            return;
        }
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        Long parentId = dataReportEntityMgr.findParentId(pid);
        while (parentId != null) {
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, dataReport.getBasicStats());
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, dataReport.getInputPresenceReport());
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, dataReport.getGeoDistributionReport());
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, dataReport.getMatchToDUNSReport());
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, dataReport.getDuplicationReport());
            parentId = dataReportEntityMgr.findParentId(parentId);
        }
    }

    @Override
    public DataReport getReadyForRollupDataReport(String customerSpace, DataReportRecord.Level level, String ownerId) {
        if (DataReportRecord.Level.Tenant.equals(level)) {
            ownerId = customerSpace;
        }
        DataReportRecord dataReportRecord = dataReportEntityMgr.findReadyForRollupDataReportRecord(level, ownerId);
        return convertRecordToDataReport(dataReportRecord);
    }

    @Override
    public void deleteDataReportUnderOwnerId(String customerSpace, DataReportRecord.Level level, String ownerId) {
        Set<Long> idToBeRemoved = getDataReportUnderOwnerId(level, ownerId);
        if (CollectionUtils.isNotEmpty(idToBeRemoved) && (idToBeRemoved.size() != 1 || !idToBeRemoved.contains(null))) {
            log.info("the is under report with level {} and ownerId {} are {}", level, ownerId, idToBeRemoved);
            dataReportEntityMgr.updateReadyForRollupToFalse(idToBeRemoved);
            // wait the replication log
            SleepUtils.sleep(200);
            // corner case: if no report in project level are ready for rollup, mark flag for tenant report to false
            Set<String> projectIds = dataReportEntityMgr.findChildrenIds(DataReportRecord.Level.Tenant, customerSpace);
            if (CollectionUtils.isEmpty(projectIds)) {
                log.info("mark ready for rollup to false for {}", customerSpace);
                Long tenantPid = dataReportEntityMgr.findDataReportPid(DataReportRecord.Level.Tenant, customerSpace);
                dataReportEntityMgr.updateReadyForRollupToFalse(Collections.singleton(tenantPid));
            }
        }
    }

    public void hardDeleteDataReportUnderOwnerId(String customerSpace, DataReportRecord.Level level, String ownerId) {
        Set<Long> idToBeTrueRemoved = getDataReportUnderOwnerId(level, ownerId);
        if (CollectionUtils.isNotEmpty(idToBeTrueRemoved) && (idToBeTrueRemoved.size() != 1 || !idToBeTrueRemoved.contains(null))) {
            log.info("the is under report with level {} and ownerId {} are {}", level, ownerId, idToBeTrueRemoved);
            dataReportEntityMgr.deleteDataReportRecords(idToBeTrueRemoved);
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5);
            retryTemplate.execute(ctx -> {
                Set<Long> remainIds = getDataReportUnderOwnerId(level, ownerId);
                if(CollectionUtils.isNotEmpty(remainIds)  && (remainIds.size() != 1 || !remainIds.contains(null))) {
                    throw new RuntimeException("report under ownerid still exist.");
                }
                return true;
            });
            // corner case: if no report in project level are ready for rollup, mark flag for tenant report to false
            Set<String> projectIds = dataReportEntityMgr.findChildrenIds(DataReportRecord.Level.Tenant, customerSpace);
            if (CollectionUtils.isEmpty(projectIds)) {
                log.info("mark ready for rollup to false for {}", customerSpace);
                Long tenantPid = dataReportEntityMgr.findDataReportPid(DataReportRecord.Level.Tenant, customerSpace);
                dataReportEntityMgr.updateReadyForRollupToFalse(Collections.singleton(tenantPid));
            }
        }
    }

    @Override
    public void updateRollupStatus(String customerSpace, DataReportRollupStatus rollupStatus) {
        DataReportRecord.Level level = DataReportRecord.Level.Tenant;
        String ownerId = CustomerSpace.shortenCustomerSpace(customerSpace);
        if (rollupStatus.getStatus() == DataReportRecord.RollupStatus.FAILED_NO_RETRY) {
            log.error(String.format("DataReportRollup for %s failed. Status code %s, message %s", customerSpace,
                    rollupStatus.getStatusCode(), rollupStatus.getStatusMessage()));
        }
        dataReportEntityMgr.updateDataReportRollupStatus(rollupStatus.getStatus(), level, ownerId);
    }

    private Set<Long> getDataReportUnderOwnerId(DataReportRecord.Level level, String ownerId){
        Set<Long> idToBeRemoved = new HashSet<>();
        switch (level) {
            case Project:
                Long projectPid = dataReportEntityMgr.findDataReportPid(level, ownerId);
                idToBeRemoved.add(projectPid);
                Set<Long> sourcePIds = dataReportEntityMgr.findPidsByParentId(projectPid);
                idToBeRemoved.addAll(sourcePIds);
                sourcePIds.forEach(sourcePId -> {
                    Set<Long> uploadPIds = dataReportEntityMgr.findPidsByParentId(sourcePId);
                    idToBeRemoved.addAll(uploadPIds);
                });
                break;
            case Source:
                Long sourcePid = dataReportEntityMgr.findDataReportPid(level, ownerId);
                idToBeRemoved.add(sourcePid);
                Set<Long> uploadPids = dataReportEntityMgr.findPidsByParentId(sourcePid);
                idToBeRemoved.addAll(uploadPids);
                break;
            case Upload:
                Long uploadPid = dataReportEntityMgr.findDataReportPid(level, ownerId);
                idToBeRemoved.add(uploadPid);
                break;
            default:
                break;
        }
        return idToBeRemoved;
    }

    private DataReportRecord getEmptyReportRecord(DataReportRecord.Level level, String ownerId) {
        DataReportRecord dataReportRecord = new DataReportRecord();
        dataReportRecord.setTenant(MultiTenantContext.getTenant());
        dataReportRecord.setLevel(level);
        dataReportRecord.setOwnerId(ownerId);
        dataReportRecord.setRefreshTime(new Date());
        return dataReportRecord;
    }

    private Long getParentPid(String customerSpace, DataReportRecord.Level currentLevel, String ownerId) {
        if (currentLevel.getParentLevel() != null) {
            String parentOwnerId = getParentOwnerId(customerSpace, currentLevel, ownerId);
            if (parentOwnerId != null) {
                Long pid = dataReportEntityMgr.findDataReportPid(currentLevel.getParentLevel(), parentOwnerId);
                if (pid != null) {
                    return pid;
                } else {
                    DataReportRecord dataReportRecord = getEmptyReportRecord(currentLevel.getParentLevel(),
                            parentOwnerId);
                    dataReportRecord.setParentId(getParentPid(customerSpace, currentLevel.getParentLevel(), parentOwnerId));
                    dataReportEntityMgr.create(dataReportRecord);
                    return dataReportRecord.getPid();
                }
            } else {
                log.warn(String.format("Cannot find parent for level %s, Id %s", currentLevel, ownerId));
                return null;
            }
        } else {
            return null;
        }
    }

    private String getParentOwnerId(String customerSpace, DataReportRecord.Level level, String ownerId) {
        Preconditions.checkNotNull(level);
        switch (level) {
            case Project:
                return (CustomerSpace.parse(customerSpace).toString());
            case Source:
                ProjectInfo projectInfo = projectService.getProjectBySourceId(customerSpace, ownerId);
                if (projectInfo != null) {
                    return projectInfo.getProjectId();
                } else {
                    log.error("Cannot find Project for Source: " + ownerId);
                }
                break;
            case Upload:
                UploadDetails uploadDetails = uploadService.getUploadByUploadId(customerSpace, ownerId, Boolean.FALSE);
                if (uploadDetails != null) {
                    return uploadDetails.getSourceId();
                } else {
                    log.error("Cannot find Source for Upload: " + ownerId);
                }
                break;
            default:
                return null;
        }
        return null;
    }


    private DataReport convertRecordToDataReport(DataReportRecord record) {
        if (record == null) {
            return null;
        }
        DataReport dataReport = new DataReport();
        dataReport.setRefreshTimestamp(record.getRefreshTime().getTime());
        dataReport.setBasicStats(record.getBasicStats());
        dataReport.setInputPresenceReport(record.getInputPresenceReport());
        dataReport.setGeoDistributionReport(record.getGeoDistributionReport());
        dataReport.setMatchToDUNSReport(record.getMatchToDUNSReport());
        dataReport.setDuplicationReport(record.getDuplicationReport());
        return dataReport;
    }
}
