package com.latticeengines.apps.dcp.service.impl;

import java.util.Date;

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
        DataReportRecord dataReportRecord = dataReportEntityMgr.findDataReportRecord(level, ownerId);
        return convertRecordToDataReport(dataReportRecord);
    }

    @Override
    public DataReportRecord getDataReportRecord(String customerSpace, DataReportRecord.Level level, String ownerId) {
        return dataReportEntityMgr.findDataReportRecord(level, ownerId);
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.BasicStats basicStats) {
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, basicStats);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setBasicStats(basicStats);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
        }
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.InputPresenceReport inputPresenceReport) {
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, inputPresenceReport);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setInputPresenceReport(inputPresenceReport);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
        }
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.GeoDistributionReport geoDistributionReport) {
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, geoDistributionReport);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setGeoDistributionReport(geoDistributionReport);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
        }
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.MatchToDUNSReport matchToDUNSReport) {
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, matchToDUNSReport);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setMatchToDUNSReport(matchToDUNSReport);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
        }
    }

    @Override
    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.DuplicationReport duplicationReport) {
        Long pid = dataReportEntityMgr.findDataReportPid(level, ownerId);
        if (pid != null) {
            dataReportEntityMgr.updateDataReportRecord(pid, duplicationReport);
        } else {
            DataReportRecord dataReportRecord = getEmptyReportRecord(level, ownerId);
            dataReportRecord.setDuplicationReport(duplicationReport);
            dataReportRecord.setParentId(getParentPid(customerSpace, level, ownerId));
            dataReportEntityMgr.create(dataReportRecord);
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
                UploadDetails uploadDetails = uploadService.getUploadByUploadId(customerSpace, ownerId);
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
