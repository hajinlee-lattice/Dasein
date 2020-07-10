package com.latticeengines.dcp.workflow.steps;

import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
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
        Map<String, DataReport> projectIdToReport = new HashMap<>();
        Map<String, DataReport> sourceIdToReport = new HashMap<>();
        Map<String, DataReport> uploadIdToReport = new HashMap<>();
        Map<DataReportRecord.Level, Map<String, DataReport>> levelMap = new HashMap<>();
        List<HdfsDataUnit> inputs = new ArrayList<>();
        prepareDataReport(level, root, parentIdToChildren, projectIdToReport, sourceIdToReport, uploadIdToReport);
        computeDataReport(level, root, mode, parentIdToChildren, levelMap);

        return null;
    }

    private void computeDataReport(DataReportRecord.Level level, String root, DataReportMode mode,
                                   Map<String, Set<String>> parentIdToChildren,
                                   Map<DataReportRecord.Level, Map<String, DataReport>> levelMap) {
        // rollup from source to root level
        DataReportRecord.Level parentLevel = DataReportRecord.Level.Source;
        DataReportRecord.Level childLevel = DataReportRecord.Level.Upload;
        DataReport rootReport = dataReportProxy.getDataReport(customerSpace.toString(), level, root);
        do {
            Map<String, DataReport> parentMap = levelMap.get(parentLevel);
            Map<String, DataReport> childMap = levelMap.get(childLevel);
            for (Map.Entry<String, DataReport> entry : parentMap.entrySet()) {
                String key = entry.getKey();
                DataReport report = entry.getValue();
                Set<String> childIds = parentIdToChildren.get(key);
                List<DataReport> dataReports = new ArrayList<>();
                for (String childId : childIds) {
                    DataReport childReport = childMap.get(childId);
                    dataReports.add(childReport);
                }
                constructParentReport(report, dataReports, mode);
            }
            childLevel = parentLevel;
            parentLevel = parentLevel.getParentLevel();
        } while(parentLevel != level);
    }

    private void constructParentReport(DataReport parentReport, List<DataReport> dataReports, DataReportMode mode) {
        switch (mode) {
            case UPDATE:
                Long time = parentReport == null ? -1L : parentReport.getSnapshotTimestamp();

                break;
            case RECOMPUTE_TREE:
                dataReports.forEach(report -> {
                    parentReport

                });

                break;
            case RECOMPUTE_ROOT:
                break;
        }
    }

    private void prepareDataReport(DataReportRecord.Level level, String root,
                                   Map<String, Set<String>> parentIdToChildren,
                                   Map<String, DataReport> projectIdToReport,
                                   Map<String, DataReport> sourceIDToReport,
                                   Map<String, DataReport> uploadIdToReport) {
        switch (level) {
            case Tenant:
                List<ProjectSummary> projects = projectProxy.getAllDCPProject(customerSpace.toString(), true);
                parentIdToChildren.put(root,
                        projects.stream().map(ProjectSummary::getProjectId).collect(Collectors.toSet()));
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
                        sourceIDToReport.put(sourceId, sourceReport);
                        List<UploadDetails> uploads = uploadProxy.getUploads(customerSpace.toString(), sourceId,
                                Upload.Status.FINISHED, false);
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
                sources.forEach(source -> {
                    String sourceId = source.getSourceId();
                    List<UploadDetails> uploads = uploadProxy.getUploads(customerSpace.toString(), sourceId,
                            Upload.Status.FINISHED, false);
                    uploads.forEach(upload -> {
                        DataReport report = dataReportProxy.getDataReport(customerSpace.toString(),
                                DataReportRecord.Level.Upload,
                                upload.getUploadId());
                        uploadIdToReport.put(upload.getUploadId(), report);
                    });
                });
                break;
            case Source:
                List<UploadDetails> uploads = uploadProxy.getUploads(customerSpace.toString(), root,
                        Upload.Status.FINISHED, false);
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
