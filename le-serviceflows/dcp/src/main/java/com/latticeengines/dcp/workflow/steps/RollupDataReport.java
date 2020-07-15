package com.latticeengines.dcp.workflow.steps;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.RollupDataReportStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.RollupDataReportConfig;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.dcp.RollupDataReportJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RollupDataReport extends RunSparkJob<RollupDataReportStepConfiguration, RollupDataReportConfig> {

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private DataReportProxy dataReportProxy;

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private SourceProxy sourceProxy;

    @Override
    protected Class<? extends AbstractSparkJob<RollupDataReportConfig>> getJobClz() {
        return RollupDataReportJob.class;
    }

    @Override
    protected RollupDataReportConfig configureJob(RollupDataReportStepConfiguration stepConfiguration) {
        String root = stepConfiguration.getRoot();
        DataReportRecord.Level level = stepConfiguration.getLevel();
        DataReportMode mode = stepConfiguration.getMode();

        Map<String, Set<String>> parentIdToChildren = new HashMap<>();
        Map<String, DataReport> tenantToReport = new HashMap<>();
        Map<String, DataReport> projectIdToReport = new HashMap<>();
        Map<String, DataReport> sourceIdToReport = new HashMap<>();
        Map<String, DataReport> uploadIdToReport = new HashMap<>();
        Map<DataReportRecord.Level, Map<String, DataReport>> levelMap = new HashMap<>();
        //List<HdfsDataUnit> inputs = new ArrayList<>();
        prepareDataReport(level, root, parentIdToChildren, tenantToReport,
                projectIdToReport, sourceIdToReport, uploadIdToReport);
        levelMap.put(DataReportRecord.Level.Tenant, tenantToReport);
        levelMap.put(DataReportRecord.Level.Project, projectIdToReport);
        levelMap.put(DataReportRecord.Level.Source, sourceIdToReport);
        levelMap.put(DataReportRecord.Level.Upload, uploadIdToReport);
        computeDataReport(level, mode, parentIdToChildren, levelMap);

        return null;
    }

    private void computeDataReport(DataReportRecord.Level level, DataReportMode mode,
                                   Map<String, Set<String>> parentIdToChildren,
                                   Map<DataReportRecord.Level, Map<String, DataReport>> levelMap) {
        // rollup from source to root level
        DataReportRecord.Level parentLevel = DataReportRecord.Level.Source;
        DataReportRecord.Level childLevel = DataReportRecord.Level.Upload;
        do {
            Map<String, DataReport> parentMap = levelMap.get(parentLevel);
            Map<String, DataReport> childMap = levelMap.get(childLevel);
            Set<String> keys = parentMap.keySet();
            for (String key : keys) {
                Set<String> childIds = parentIdToChildren.get(key);
                Map<String, DataReport> childOwnerIdToReport = new HashMap<>();
                for (String childId : childIds) {
                    DataReport childReport = childMap.get(childId);
                    childOwnerIdToReport.put(childId, childReport);
                }
                DataReport parentReport = parentMap.get(key);
                DataReport updatedParentReport = constructParentReport(key, parentLevel, parentReport, childLevel,
                        childOwnerIdToReport, mode);
                if (!DataReportMode.UPDATE.equals(mode)) {
                    parentMap.put(key, updatedParentReport);
                }
            }
            childLevel = parentLevel;
            parentLevel = parentLevel.getParentLevel();
        } while(childLevel != level);
    }

    private DataReport constructParentReport(String parentOwnerId, DataReportRecord.Level parentLevel,
                                       DataReport parentReport, DataReportRecord.Level childLevel,
                                       Map<String, DataReport> childOwnerIdToReport, DataReportMode mode) {
        DataReport updatedParentReport = new DataReport();
        switch (mode) {
            case UPDATE:
                DunsCountCache parentCache = dataReportProxy.getDunsCount(customerSpace.toString(), parentLevel,
                        parentOwnerId);
                Date parentTime = parentCache.getSnapshotTimestamp();
                boolean needUpdate = false;
                if (parentTime == null) {
                    needUpdate = true;
                }
                if (!needUpdate) {
                    Set<String> childOwnerIds = childOwnerIdToReport.keySet();
                    for (String childOwnerId : childOwnerIds) {
                        DunsCountCache childCache = dataReportProxy.getDunsCount(customerSpace.toString(), childLevel
                                , childOwnerId);
                        if (childCache.getSnapshotTimestamp() != null && parentTime.before(childCache.getSnapshotTimestamp())) {
                            needUpdate = true;
                        }
                    }
                }
                if (needUpdate) {
                    childOwnerIdToReport.forEach((key, value) -> updatedParentReport.combineReport(value));
                    dataReportProxy.updateDataReport(customerSpace.toString(), parentLevel, parentOwnerId, updatedParentReport);
                } else {
                    return parentReport;
                }
                break;
            case RECOMPUTE_TREE:
                childOwnerIdToReport.forEach((key, value) -> updatedParentReport.combineReport(value));
                dataReportProxy.updateDataReport(customerSpace.toString(), parentLevel, parentOwnerId, updatedParentReport);
                break;
            case RECOMPUTE_ROOT:
                childOwnerIdToReport.forEach((key, value) -> updatedParentReport.combineReport(value));
                break;
        }

        return updatedParentReport;
    }

    private void prepareDataReport(DataReportRecord.Level level, String root,
                                   Map<String, Set<String>> parentIdToChildren,
                                   Map<String, DataReport> tenantIdToReport,
                                   Map<String, DataReport> projectIdToReport,
                                   Map<String, DataReport> sourceIdToReport,
                                   Map<String, DataReport> uploadIdToReport) {
        switch (level) {
            case Tenant:
                List<ProjectSummary> projects = projectProxy.getAllDCPProject(customerSpace.toString(), true);
                parentIdToChildren.put(root,
                        projects.stream().map(ProjectSummary::getProjectId).collect(Collectors.toSet()));
                DataReport tenantReport = dataReportProxy.getDataReport(customerSpace.toString(),
                    DataReportRecord.Level.Tenant, root);
                tenantIdToReport.put(customerSpace.toString(), tenantReport);
                projects.forEach(projectSummary -> {
                    String projectId = projectSummary.getProjectId();
                    DataReport projectReport = dataReportProxy.getDataReport(customerSpace.toString(),
                            DataReportRecord.Level.Project, projectId);
                    projectIdToReport.put(projectId, projectReport);
                    List<Source> sources = projectSummary.getSources();
                    parentIdToChildren.put(projectId,
                            sources.stream().map(Source::getSourceId).collect(Collectors.toSet()));
                    sources.forEach(source -> {
                        String sourceId = source.getSourceId();
                        DataReport sourceReport = dataReportProxy.getDataReport(customerSpace.toString(),
                                DataReportRecord.Level.Source, sourceId);
                        sourceIdToReport.put(sourceId, sourceReport);
                        List<UploadDetails> uploads = uploadProxy.getUploads(customerSpace.toString(), sourceId,
                                Upload.Status.FINISHED, false);
                        parentIdToChildren.put(sourceId,
                                uploads.stream().map(UploadDetails::getUploadId).collect(Collectors.toSet()));
                        uploads.forEach(upload -> {
                            DataReport report = dataReportProxy.getDataReport(customerSpace.toString(),
                                    DataReportRecord.Level.Upload,
                                    upload.getUploadId());
                            if (report != null) {
                                uploadIdToReport.put(upload.getUploadId(), report);
                            }
                        });
                    });
                });
                break;
            case Project:
                ProjectDetails project = projectProxy.getDCPProjectByProjectId(customerSpace.toString(),
                        root, Boolean.TRUE);
                List<Source> sources = project.getSources();
                parentIdToChildren.put(project.getProjectId(),
                        sources.stream().map(Source::getSourceId).collect(Collectors.toSet()));
                DataReport projectReport = dataReportProxy.getDataReport(customerSpace.toString(),
                        DataReportRecord.Level.Project, root);
                projectIdToReport.put(root, projectReport);
                sources.forEach(source -> {
                    String sourceId = source.getSourceId();
                    DataReport sourceReport = dataReportProxy.getDataReport(customerSpace.toString(),
                            DataReportRecord.Level.Source, sourceId);
                    sourceIdToReport.put(sourceId, sourceReport);
                    List<UploadDetails> uploads = uploadProxy.getUploads(customerSpace.toString(), sourceId,
                            Upload.Status.FINISHED, false);
                    parentIdToChildren.put(sourceId,
                            uploads.stream().map(UploadDetails::getUploadId).collect(Collectors.toSet()));
                    uploads.forEach(upload -> {
                        DataReport report = dataReportProxy.getDataReport(customerSpace.toString(),
                                DataReportRecord.Level.Upload,
                                upload.getUploadId());
                        uploadIdToReport.put(upload.getUploadId(), report);
                    });
                });
                break;
            case Source:
                DataReport sourceReport = dataReportProxy.getDataReport(customerSpace.toString(),
                        DataReportRecord.Level.Source, root);
                sourceIdToReport.put(root, sourceReport);
                List<UploadDetails> uploads = uploadProxy.getUploads(customerSpace.toString(), root,
                        Upload.Status.FINISHED, false);
                parentIdToChildren.put(root,
                        uploads.stream().map(UploadDetails::getUploadId).collect(Collectors.toSet()));
                uploads.forEach(upload -> {
                    DataReport report = dataReportProxy.getDataReport(customerSpace.toString(),
                            DataReportRecord.Level.Upload,
                            upload.getUploadId());
                    uploadIdToReport.put(upload.getUploadId(), report);
                });
                break;
            default:
                break;
        }
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {

    }
}
