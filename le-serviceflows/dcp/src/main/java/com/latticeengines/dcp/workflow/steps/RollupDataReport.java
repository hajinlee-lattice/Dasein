package com.latticeengines.dcp.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchedDuns;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.RollupDataReportStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.RollupDataReportConfig;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.dcp.RollupDataReportJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RollupDataReport extends RunSparkJob<RollupDataReportStepConfiguration, RollupDataReportConfig> {

    private static final Logger log = LoggerFactory.getLogger(RollupDataReport.class);

    @Inject
    private DataReportProxy dataReportProxy;

    // this object records the owner id that need to update report and dunsCount from source level to the above
    // which also corresponds to the generated targets and dupReports
    private List<String> updatedOwnerIds;

    // this object record owner id to level mapping
    private Map<String, Pair<DataReportRecord.Level, Date>> updatedOwnerIdToLevelAndDate;

    @Override
    protected Class<? extends AbstractSparkJob<RollupDataReportConfig>> getJobClz() {
        return RollupDataReportJob.class;
    }

    @Override
    protected RollupDataReportConfig configureJob(RollupDataReportStepConfiguration stepConfiguration) {
        String rootId = stepConfiguration.getRootId();
        DataReportRecord.Level level = stepConfiguration.getLevel();
        DataReportMode mode = stepConfiguration.getMode();

        Map<String, Set<String>> parentIdToChildren = new HashMap<>();
        Map<String, DataReport> tenantToReport = new HashMap<>();
        Map<String, DataReport> projectIdToReport = new HashMap<>();
        Map<String, DataReport> sourceIdToReport = new HashMap<>();
        Map<String, DataReport> uploadIdToReport = new HashMap<>();
        Map<DataReportRecord.Level, Map<String, DataReport>> levelMap = new HashMap<>();
        List<DataUnit> inputs = new ArrayList<>();
        prepareDataReport(level, rootId, parentIdToChildren, tenantToReport,
                projectIdToReport, sourceIdToReport, uploadIdToReport);
        levelMap.put(DataReportRecord.Level.Tenant, tenantToReport);
        levelMap.put(DataReportRecord.Level.Project, projectIdToReport);
        levelMap.put(DataReportRecord.Level.Source, sourceIdToReport);
        levelMap.put(DataReportRecord.Level.Upload, uploadIdToReport);

        updatedOwnerIds = new ArrayList<>();
        updatedOwnerIdToLevelAndDate = new HashMap<>();

        RollupDataReportConfig config = new RollupDataReportConfig();
        config.setMatchedDunsAttr(MatchedDuns);
        config.setMode(mode);
        if (mode == DataReportMode.RECOMPUTE_ROOT) {
            updatedOwnerIds.add(rootId);
            Date date = computeReportAndReturnSnapShotTime(uploadIdToReport, rootId, level, inputs);
            updatedOwnerIdToLevelAndDate.put(rootId, Pair.of(level, date));
            config.setInput(inputs);
            config.setNumTargets(1);
        } else {
            Map<String, Table> ownerIdToDunsCount = new HashMap<>();
            computeDataReport(level, mode, parentIdToChildren, levelMap, updatedOwnerIds, updatedOwnerIdToLevelAndDate,
                    ownerIdToDunsCount);
            // compute the input list, the input tables are duns count table
            // the target table size are key number size of owner ids
            int index = 0;
            Map<String, Integer> inputOwnerIdToIndex = new HashMap<>();
            for (Map.Entry<String, Table> entry : ownerIdToDunsCount.entrySet()) {
                String ownerId = entry.getKey();
                Table dunsCount = entry.getValue();
                inputs.add(dunsCount.toHdfsDataUnit(String.format("input%s", index)));
                inputOwnerIdToIndex.put(ownerId, index);
                index++;
            }

            config.setInput(inputs);
            config.setInputOwnerIdToIndex(inputOwnerIdToIndex);
            config.setUpdatedOwnerIds(updatedOwnerIds);
            config.setParentIdToChildren(parentIdToChildren);
            config.setNumTargets(updatedOwnerIds.size());
        }
        return config;
    }

    private void computeDataReport(DataReportRecord.Level level,
                                   DataReportMode mode,
                                   Map<String, Set<String>> parentIdToChildren,
                                   Map<DataReportRecord.Level, Map<String, DataReport>> levelMap,
                                   List<String> updatedOwnerIds,
                                   Map<String, Pair<DataReportRecord.Level, Date>> updatedOwnerIdToLevelAndDate,
                                   Map<String, Table> ownerIdToDunsCount) {
        // rollup from source to root level
        DataReportRecord.Level parentLevel = DataReportRecord.Level.Source;
        DataReportRecord.Level childLevel = DataReportRecord.Level.Upload;
        do {
            Map<String, DataReport> parentMap = levelMap.get(parentLevel);
            Map<String, DataReport> childMap = levelMap.get(childLevel);
            Set<String> parentOwnerIds = parentMap.keySet();

            for (String parentOwnerId : parentOwnerIds) {
                Set<String> childIds = parentIdToChildren.get(parentOwnerId);
                Map<String, DataReport> childOwnerIdToReport = new HashMap<>();
                for (String childId : childIds) {
                    DataReport childReport = childMap.get(childId);
                    childOwnerIdToReport.put(childId, childReport);
                }
                DataReport parentReport = parentMap.get(parentOwnerId);

                Pair<Date, DataReport> result = constructParentReport(parentOwnerId, parentLevel, parentReport,
                        childLevel, childOwnerIdToReport, updatedOwnerIdToLevelAndDate, ownerIdToDunsCount,
                        mode);

                // write report back if needed
                if (result != null && result.getLeft() != null) {
                    parentMap.put(parentOwnerId, result.getRight());
                    // update whether the parental owner id needs to be updated
                    updatedOwnerIds.add(parentOwnerId);
                    updatedOwnerIdToLevelAndDate.put(parentOwnerId, Pair.of(parentLevel, result.getLeft()));
                }
            }
            childLevel = parentLevel;
            parentLevel = parentLevel.getParentLevel();
        } while(childLevel != level);
    }

    private Pair<Date, DataReport> constructParentReport(String parentOwnerId,
                                                         DataReportRecord.Level parentLevel,
                                                         DataReport parentReport, DataReportRecord.Level childLevel,
                                                         Map<String, DataReport> childOwnerIdToReport,
                                                         Map<String, Pair<DataReportRecord.Level, Date>> updatedOwnerIdToLevelAndDate,
                                                         Map<String, Table> ownerIdToDunsCount,
                                                         DataReportMode mode) {
        DataReport updatedParentReport = initializeReport();
        // only when the parent node need to update, return the snapshot time
        DunsCountCache parentCache = dataReportProxy.getDunsCount(customerSpace.toString(), parentLevel,
                parentOwnerId);
        Date parentTime = parentCache.getSnapshotTimestamp();
        if (parentTime == null) {
            // this shouldn't happen if everything is ok
            log.info(String.format("the duns count is not registered for rootId %s and level %s", parentOwnerId,
                    parentLevel));
            throw new RuntimeException(String.format("the duns count is not registered for rootId %s and " +
                    "level %s", parentOwnerId, parentLevel));
        }
        Date maxChildDate = parentTime;
        switch (mode) {
            case UPDATE:
                maxChildDate = parentTime;
                // traverse the adjacent child node to judge whether to update parental node
                Set<String> childOwnerIds = childOwnerIdToReport.keySet();
                for (String childOwnerId : childOwnerIds) {
                    if (updatedOwnerIdToLevelAndDate.containsKey(childOwnerId)) {
                        Pair<DataReportRecord.Level, Date> pair = updatedOwnerIdToLevelAndDate.get(childOwnerId);
                        if (maxChildDate.before(pair.getRight())) {
                            maxChildDate = pair.getRight();
                        }
                    } else {
                        DunsCountCache childCache = dataReportProxy.getDunsCount(customerSpace.toString(),
                                childLevel, childOwnerId);
                        if (childCache.getSnapshotTimestamp() == null ||
                                StringUtils.isBlank(childCache.getDunsCountTableName())) {
                            throw new RuntimeException(String.format("the duns count is not registered for rootId %s and " +
                                    "level %s", childOwnerId, childLevel));
                        }
                        if (maxChildDate.before(childCache.getSnapshotTimestamp())) {
                            maxChildDate= childCache.getSnapshotTimestamp();
                        }
                        Table dunsCountTable = metadataProxy.getTable(customerSpace.toString(),
                                childCache.getDunsCountTableName());
                        Preconditions.checkNotNull(dunsCountTable, String.format("duns count table %s shouldn't be null",
                                childCache.getDunsCountTableName()));
                        ownerIdToDunsCount.put(childOwnerId, dunsCountTable);
                    }
                }
                if (parentTime.before(maxChildDate)) {
                    childOwnerIdToReport.forEach((childOwnerId, childReport) -> updatedParentReport.combineReport(childReport));
                    dataReportProxy.updateDataReport(customerSpace.toString(), parentLevel, parentOwnerId, updatedParentReport);
                    return Pair.of(maxChildDate, updatedParentReport);
                } else {
                    // the parent node needn't sub node's data
                    childOwnerIds.forEach(ownerIdToDunsCount::remove);
                    // pass one empty object to refresh the time
                    dataReportProxy.updateDataReport(customerSpace.toString(), parentLevel, parentOwnerId, new DataReport());
                    return Pair.of(null, parentReport);
                }
            case RECOMPUTE_TREE:
                for (Map.Entry<String, DataReport> entry : childOwnerIdToReport.entrySet()) {
                    String childOwnerId = entry.getKey();
                    DataReport childReport = entry.getValue();
                    updatedParentReport.combineReport(childReport);
                    if (updatedOwnerIdToLevelAndDate.containsKey(childOwnerId)) {
                        Pair<DataReportRecord.Level, Date> pair = updatedOwnerIdToLevelAndDate.get(childOwnerId);
                        if (maxChildDate.before(pair.getRight())) {
                            maxChildDate = pair.getRight();
                        }
                    }
                    DunsCountCache childCache = dataReportProxy.getDunsCount(customerSpace.toString(),
                            childLevel, childOwnerId);
                    Date childSnapshotTime = childCache.getSnapshotTimestamp();
                    if (childSnapshotTime == null ||
                            StringUtils.isBlank(childCache.getDunsCountTableName())) {
                        throw new RuntimeException(String.format("the duns count is not registered for rootId %s and " +
                                "level %s", childOwnerId, childLevel));
                    }
                    if (maxChildDate.before(childSnapshotTime)) {
                        maxChildDate = childSnapshotTime;
                    }
                    // only need duns count table in upload level
                    if (childLevel == DataReportRecord.Level.Upload) {
                        Table dunsCountTable = metadataProxy.getTable(customerSpace.toString(),
                                childCache.getDunsCountTableName());
                        Preconditions.checkNotNull(dunsCountTable, String.format("duns count table %s shouldn't be " +
                                "null", childCache.getDunsCountTableName()));
                        ownerIdToDunsCount.put(childOwnerId, dunsCountTable);
                    }
                }
                dataReportProxy.updateDataReport(customerSpace.toString(), parentLevel, parentOwnerId, updatedParentReport);
                return Pair.of(maxChildDate, updatedParentReport);
                default:
                    return null;
        }
    }

    private Date computeReportAndReturnSnapShotTime(Map<String, DataReport> uploadIdToReport, String rootId,
                                 DataReportRecord.Level level, List<DataUnit> inputs) {
        DataReport updatedParentReport = initializeReport();
        DunsCountCache parentCache = dataReportProxy.getDunsCount(customerSpace.toString(), level,
                rootId);
        Date parentTime = parentCache.getSnapshotTimestamp();
        if (parentTime == null) {
            // this shouldn't happen if everything is ok
            log.info(String.format("the duns count is not registered for rootId %s and level %s", rootId,
                    level));
            throw new RuntimeException(String.format("the duns count is not registered for rootId %s and " +
                    "level %s", rootId, level));
        }
        Date maxChildDate = parentTime;
        // in RECOMPUTE_ROOT, only pass the upload level node
        int index = 0;

        for (Map.Entry<String, DataReport> entry : uploadIdToReport.entrySet()) {
            String ownerId = entry.getKey();
            DataReport uploadReport = entry.getValue();
            updatedParentReport.combineReport(uploadReport);
            DunsCountCache childCache = dataReportProxy.getDunsCount(customerSpace.toString(),
                    DataReportRecord.Level.Upload, ownerId);
            if (childCache.getSnapshotTimestamp() == null ||
                    StringUtils.isBlank(childCache.getDunsCountTableName())) {
                throw new RuntimeException(String.format("the duns count is not registered for rootId %s and " +
                        "level %s", ownerId, DataReportRecord.Level.Upload));
            }
            if (maxChildDate.before(childCache.getSnapshotTimestamp())) {
                maxChildDate = childCache.getSnapshotTimestamp();
            }
            Table dunsCountTable = metadataProxy.getTable(customerSpace.toString(),
                    childCache.getDunsCountTableName());
            Preconditions.checkNotNull(dunsCountTable, String.format("duns count table %s shouldn't be " +
                    "null", childCache.getDunsCountTableName()));
            inputs.add(dunsCountTable.toHdfsDataUnit(String.format("input%s", index)));
            index++;
        }
        dataReportProxy.updateDataReport(customerSpace.toString(), level, rootId, updatedParentReport);
        return maxChildDate;
    }

    private DataReport initializeReport() {
        DataReport report = new DataReport();
        report.setInputPresenceReport(new DataReport.InputPresenceReport());
        DataReport.BasicStats stats = new DataReport.BasicStats();
        stats.setSuccessCnt(0L);
        stats.setErrorCnt(0L);
        stats.setPendingReviewCnt(0L);
        stats.setMatchedCnt(0L);
        stats.setTotalSubmitted(0L);
        stats.setUnmatchedCnt(0L);
        report.setBasicStats(stats);
        report.setDuplicationReport(new DataReport.DuplicationReport());
        report.setGeoDistributionReport(new DataReport.GeoDistributionReport());
        report.setMatchToDUNSReport(new DataReport.MatchToDUNSReport());
        return report;
    }

    private void prepareDataReport(DataReportRecord.Level level, String rootId,
                                   Map<String, Set<String>> parentIdToChildren,
                                   Map<String, DataReport> tenantIdToReport,
                                   Map<String, DataReport> projectIdToReport,
                                   Map<String, DataReport> sourceIdToReport,
                                   Map<String, DataReport> uploadIdToReport) {
        switch (level) {
            case Tenant:
                DataReport tenantReport = dataReportProxy.getDataReport(customerSpace.toString(),
                        DataReportRecord.Level.Tenant, rootId);
                Preconditions.checkNotNull(tenantReport, String.format("no report found for %s", rootId));
                tenantIdToReport.put(rootId, tenantReport);
                Set<String> projectIds = dataReportProxy.getSubOwnerIds(customerSpace.toString(), level, rootId);
                projectIds.forEach(projectId -> {
                    DataReport projectReport = dataReportProxy.getDataReport(customerSpace.toString(),
                            DataReportRecord.Level.Project, projectId);
                    Preconditions.checkNotNull(projectReport, String.format("no report found for %s ", projectId));
                    projectIdToReport.put(projectId, projectReport);
                    Set<String> sourceIds = dataReportProxy.getSubOwnerIds(customerSpace.toString(),
                            DataReportRecord.Level.Project, projectId);
                    parentIdToChildren.put(projectId, sourceIds);
                    sourceIds.forEach(sourceId -> {
                        DataReport sourceReport = dataReportProxy.getDataReport(customerSpace.toString(),
                                DataReportRecord.Level.Source, sourceId);
                        Preconditions.checkNotNull(sourceReport, String.format("no report found for %s ", sourceId));
                        sourceIdToReport.put(sourceId, sourceReport);
                        Set<String> uploadIds = dataReportProxy.getSubOwnerIds(customerSpace.toString(),
                                DataReportRecord.Level.Source, sourceId);
                        parentIdToChildren.put(sourceId, uploadIds);
                        uploadIds.forEach(uploadId -> {
                            DataReport report = dataReportProxy.getDataReport(customerSpace.toString(),
                                    DataReportRecord.Level.Upload, uploadId);
                            Preconditions.checkNotNull(report, String.format("no report found for %s ", uploadId));
                            uploadIdToReport.put(uploadId, report);
                        });
                    });
                });
                break;
            case Project:
                Set<String> sourceIds = dataReportProxy.getSubOwnerIds(customerSpace.toString(),
                        DataReportRecord.Level.Project, rootId);
                parentIdToChildren.put(rootId, sourceIds);
                DataReport projectReport = dataReportProxy.getDataReport(customerSpace.toString(),
                        DataReportRecord.Level.Project, rootId);
                Preconditions.checkNotNull(projectReport, String.format("no report found for %s", rootId));
                projectIdToReport.put(rootId, projectReport);
                sourceIds.forEach(sourceId -> {
                    DataReport sourceReport = dataReportProxy.getDataReport(customerSpace.toString(),
                            DataReportRecord.Level.Source, sourceId);
                    Preconditions.checkNotNull(sourceReport, String.format("no report found for %s ", sourceId));
                    sourceIdToReport.put(sourceId, sourceReport);
                    Set<String> uploadIds = dataReportProxy.getSubOwnerIds(customerSpace.toString(),
                            DataReportRecord.Level.Source, sourceId);
                    parentIdToChildren.put(sourceId, uploadIds);
                    uploadIds.forEach(uploadId -> {
                        DataReport report = dataReportProxy.getDataReport(customerSpace.toString(),
                                DataReportRecord.Level.Upload, uploadId);
                        Preconditions.checkNotNull(report, String.format("no report found for %s ", uploadId));
                        uploadIdToReport.put(uploadId, report);
                    });
                });
                break;
            case Source:
                DataReport sourceReport = dataReportProxy.getDataReport(customerSpace.toString(),
                        DataReportRecord.Level.Source, rootId);
                Preconditions.checkNotNull(sourceReport, String.format("no report found for %s ", rootId));
                sourceIdToReport.put(rootId, sourceReport);
                Set<String> uploadIds = dataReportProxy.getSubOwnerIds(customerSpace.toString(),
                        DataReportRecord.Level.Source, rootId);
                parentIdToChildren.put(rootId, uploadIds);
                uploadIds.forEach(uploadId -> {
                    DataReport report = dataReportProxy.getDataReport(customerSpace.toString(),
                            DataReportRecord.Level.Upload, uploadId);
                    Preconditions.checkNotNull(report, String.format("no report found for %s ", uploadId));
                    uploadIdToReport.put(uploadId, report);

                });
                break;
            default:
                break;
        }
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        List<DataReport.DuplicationReport> dupReports = JsonUtils.convertList(JsonUtils.deserialize(result.getOutput(),
                List.class), DataReport.DuplicationReport.class);
        List<HdfsDataUnit> units = result.getTargets();
        int index = 0;
        for (String ownerId : updatedOwnerIds) {
            Pair<DataReportRecord.Level, Date> pair = updatedOwnerIdToLevelAndDate.get(ownerId);
            DataReportRecord.Level level = pair.getLeft();
            Date snapshotTime = pair.getRight();
            HdfsDataUnit unit = units.get(index);
            // register duns count cache
            String dunsCountTableName = NamingUtils.timestamp(String.format("dunsCount_%s", ownerId));
            Table dunsCount = toTable(dunsCountTableName, null, unit);
            metadataProxy.createTable(configuration.getCustomerSpace().toString(), dunsCountTableName, dunsCount);
            registerTable(dunsCountTableName);

            DunsCountCache cache = new DunsCountCache();
            cache.setDunsCountTableName(dunsCountTableName);
            cache.setSnapshotTimestamp(snapshotTime);
            dataReportProxy.registerDunsCount(configuration.getCustomerSpace().toString(), level,
                    ownerId, cache);

            // update duplication report
            DataReport.DuplicationReport dupReport = dupReports.get(index);
            dataReportProxy.updateDataReport(customerSpace.toString(), level, ownerId, dupReport);
            index++;
        }
    }
}
