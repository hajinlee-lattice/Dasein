package com.latticeengines.apps.dcp.service.impl;

import java.util.Date;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.DataReportEntityMgr;
import com.latticeengines.apps.dcp.service.DataReportService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.UploadDetails;

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
        Long parentId = dataReportEntityMgr.findParentId(pid);
        while (parentId != null) {
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, basicStats);
            parentId = dataReportEntityMgr.findParentId(parentId);
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
        Long parentId = dataReportEntityMgr.findParentId(pid);
        while (parentId != null) {
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, inputPresenceReport);
            parentId = dataReportEntityMgr.findParentId(parentId);
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
        Long parentId = dataReportEntityMgr.findParentId(pid);
        while (parentId != null) {
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, geoDistributionReport);
            parentId = dataReportEntityMgr.findParentId(parentId);
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
        Long parentId = dataReportEntityMgr.findParentId(pid);
        while (parentId != null) {
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, matchToDUNSReport);
            parentId = dataReportEntityMgr.findParentId(parentId);
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
        Long parentId = dataReportEntityMgr.findParentId(pid);
        while (parentId != null) {
            dataReportEntityMgr.updateDataReportRecordIfNull(parentId, duplicationReport);
            parentId = dataReportEntityMgr.findParentId(parentId);
        }
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
