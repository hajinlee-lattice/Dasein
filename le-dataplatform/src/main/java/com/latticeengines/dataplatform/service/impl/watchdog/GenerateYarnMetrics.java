package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.util.Times;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.entitymanager.YarnMetricGeneratorInfoEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.metrics.YarnMetricGeneratorInfo;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.yarn.exposed.runtime.metric.CompletedJobByAppIdMeasurement;
import com.latticeengines.yarn.exposed.runtime.metric.CompletedJobByAppIdMetric;
import com.latticeengines.yarn.exposed.runtime.metric.CompletedJobMeasurement;
import com.latticeengines.yarn.exposed.runtime.metric.CompletedJobMetric;
import com.latticeengines.yarn.exposed.runtime.metric.InProgressJobMeasurement;
import com.latticeengines.yarn.exposed.runtime.metric.InProgressJobMetric;
import com.latticeengines.yarn.exposed.runtime.metric.QueueMeasurement;
import com.latticeengines.yarn.exposed.runtime.metric.QueueMetric;
import com.latticeengines.yarn.exposed.service.impl.JobNameServiceImpl;

@Component("generateYarnMetrics")
public class GenerateYarnMetrics extends WatchdogPlugin {

    private static final Logger log = LoggerFactory.getLogger(GenerateYarnMetrics.class);

    private static Set<String> workflowJobNames = Sets.newHashSet( //
            "bulkMatchWorkflow", //
            "cascadingBulkMatchWorkflow", //
            "importAndRTSBulkScoreWorkflow", //
            "importMatchAndModelWorkflow", //
            "importMatchAndScoreWorkflow", //
            "importVdbTableAndPublishWorkflow", //
            "ingestionWorkflow", //
            "modelAndEmailWorkflow", //
            "modelWorkflow", //
            "pmmlModelWorkflow", //
            "transformationWorkflow", //
            "cdlDataFeedImportWorkflow", //
            "consolidateAndPublishWorkflow", //
            "processAnalyzeWorkflow", //
            "profileAndPublishWorkflow", //
            "redshiftPublishWorkflow", //
            "publishWorkflow", //
            "rtsBulkScoreWorkflow", //
            "scoreWorkflow");

    private static Set<ReportPurpose> reportsPurposes = Sets.newHashSet( //
            ReportPurpose.IMPORT_DATA_SUMMARY, //
            ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY, //
            ReportPurpose.PUBLISH_DATA_SUMMARY);

    private static Splitter nameSplitter = Splitter.on(JobNameServiceImpl.JOBNAME_DELIMITER).trimResults()
            .omitEmptyStrings();

    private static final String MAIN = "main";

    @Autowired
    private MetricService metricService;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private YarnMetricGeneratorInfoEntityMgr yarnMetricGeneratorInfoEntityMgr;

    @Value("${dataplatform.completedjob.querylimit:1000}")
    private int completedJobQueryLimit;

    public GenerateYarnMetrics() {
        register(this);
    }

    @Override
    public void run() {
        generateQueueMetrics();

        generateInProgressJobMetrics();

        YarnMetricGeneratorInfo yarnMetricGeneratorInfo = yarnMetricGeneratorInfoEntityMgr.findByName(MAIN);
        if (yarnMetricGeneratorInfo == null) {
            yarnMetricGeneratorInfo = new YarnMetricGeneratorInfo();
            yarnMetricGeneratorInfo.setName(MAIN);
            // bootstrap previous 1 hour
            yarnMetricGeneratorInfo.setFinishedTimeBegin(System.currentTimeMillis() - 3600000L);
            yarnMetricGeneratorInfoEntityMgr.create(yarnMetricGeneratorInfo);
        }

        long finishedTimeBegin = yarnMetricGeneratorInfo.getFinishedTimeBegin() + 1;
        long newFinishedTimeBegin = generateCompletedJobMetrics(finishedTimeBegin, FinalApplicationStatus.SUCCEEDED);
        generateCompletedJobMetrics(finishedTimeBegin, FinalApplicationStatus.FAILED);
        generateCompletedJobMetrics(finishedTimeBegin, FinalApplicationStatus.KILLED);
        yarnMetricGeneratorInfo.setFinishedTimeBegin(newFinishedTimeBegin);
        yarnMetricGeneratorInfoEntityMgr.update(yarnMetricGeneratorInfo);
    }

    private long generateCompletedJobMetrics(long finishedTimeBegin, FinalApplicationStatus finalAppStatus) {
        String query = String.format("finalStatus=%s&finishedTimeBegin=%d&limit=%d", finalAppStatus, finishedTimeBegin,
                completedJobQueryLimit);
        log.info(query);
        GetApplicationsRequest request = GetApplicationsRequest.newInstance();
        request.setFinishRange(finishedTimeBegin, Long.MAX_VALUE);
        request.setLimit(completedJobQueryLimit);
        List<ApplicationReport> appReports = getYarnService().getApplications(request);
        log.info(String.format("Processing %d apps with %s status", appReports.size(), finalAppStatus));

        long finishTime = finishedTimeBegin;
        for (ApplicationReport appReport : appReports) {
            // Evaluate all (not just the workflow-driver) apps for the
            // finishTime
            if (appReport.getFinalApplicationStatus() != finalAppStatus) {
                continue;
            }
            finishTime = Math.max(finishTime, appReport.getFinishTime());

            List<String> splits = nameSplitter.splitToList(appReport.getName());
            String suffix = splits.get(splits.size() - 1);
            if (!workflowJobNames.contains(suffix)) {
                // skip non-workflow-driver jobs
                continue;
            }

            Map<String, Object> fieldMap = generateCompletedJobFieldMap(appReport);

            CompletedJobMetric completedJobMetric = new CompletedJobMetric();
            completedJobMetric.setQueue(appReport.getQueue());
            completedJobMetric.setFinalAppStatus(finalAppStatus.name());
            completedJobMetric.setJobName(suffix);
            completedJobMetric.setTenantId(splits.get(0));
            CompletedJobMeasurement completedJobMeasurement = new CompletedJobMeasurement(completedJobMetric);
            metricService.write(MetricDB.LEYARN, completedJobMeasurement, fieldMap);

            CompletedJobByAppIdMetric completedJobByAppIdMetric = new CompletedJobByAppIdMetric();
            completedJobByAppIdMetric.setApplicationId(appReport.getApplicationId().toString());
            CompletedJobByAppIdMeasurement completedJobByAppIdMeasurement = new CompletedJobByAppIdMeasurement(
                    completedJobByAppIdMetric);
            metricService.write(MetricDB.LEYARN, completedJobByAppIdMeasurement, fieldMap);
        }
        return finishTime;
    }

    private void generateInProgressJobMetrics() {
        List<ApplicationReport> runningAppReports = getYarnService()
                .getRunningApplications(GetApplicationsRequest.newInstance());

        log.info(String.format("Processing %d apps in progress", runningAppReports.size()));
        for (ApplicationReport runningAppReport : runningAppReports) {
            List<String> splits = nameSplitter.splitToList(runningAppReport.getName());
            String suffix = splits.get(splits.size() - 1);
            InProgressJobMetric jobMetric = new InProgressJobMetric();
            ApplicationResourceUsageReport resourceReport = runningAppReport.getApplicationResourceUsageReport();
            Resource usedResources = resourceReport.getUsedResources();
            jobMetric.setApplicationId(runningAppReport.getApplicationId().toString());
            jobMetric.setQueue(runningAppReport.getQueue());
            jobMetric.setAllocatedMB(usedResources.getMemory());
            jobMetric.setAllocatedVCores(usedResources.getVirtualCores());
            jobMetric.setMemorySeconds((int) resourceReport.getMemorySeconds());
            jobMetric.setRunningContainers(resourceReport.getNumUsedContainers());
            jobMetric.setVcoreSeconds((int) resourceReport.getVcoreSeconds());
            jobMetric.setElapsedTimeSec(Integer.valueOf(
                    (int) Times.elapsed(runningAppReport.getStartTime(), runningAppReport.getFinishTime()) / 1000));

            jobMetric.setFinalAppStatus(FinalApplicationStatus.UNDEFINED.name());

            String split = splits.get(0);
            if (!split.contains("[") && !split.contains("]")
                    && (splits.size() > 1 || split.toLowerCase().endsWith("playmaker"))) {
                jobMetric.setTenantId(split);
            } else {
                log.debug("Job does not contain proper tenant name:" + split + " split from: "
                        + runningAppReport.getName());
                jobMetric.setTenantId("NA");
            }

            if (workflowJobNames.contains(suffix)) {
                jobMetric.setIsWorkflowDriver(true);
                jobMetric.setJobName(suffix);
            } else {
                jobMetric.setIsWorkflowDriver(false);
                jobMetric.setJobName("NA");
            }

            if (runningAppReport.getYarnApplicationState() == YarnApplicationState.RUNNING) {
                jobMetric.setIsRunning(true);
                jobMetric.setIsWaiting(false);
            } else {
                jobMetric.setIsRunning(false);
                jobMetric.setIsWaiting(true);
            }

            InProgressJobMeasurement inProgressJobMeasurement = new InProgressJobMeasurement(jobMetric);
            metricService.write(MetricDB.LEYARN, inProgressJobMeasurement);
        }
    }

    private void generateQueueMetrics() {
        CapacitySchedulerInfo capacitySchedulerInfo = getYarnService().getCapacitySchedulerInfo();

        for (CapacitySchedulerQueueInfo capacitySchedulerQueueInfo : capacitySchedulerInfo.getQueues()
                .getQueueInfoList()) {
            CapacitySchedulerLeafQueueInfo capacitySchedulerLeafQueueInfo = (CapacitySchedulerLeafQueueInfo) capacitySchedulerQueueInfo;
            QueueMetric queueMetric = new QueueMetric();
            queueMetric.setQueue(capacitySchedulerLeafQueueInfo.getQueueName());
            queueMetric.setActiveApplications(capacitySchedulerLeafQueueInfo.getNumActiveApplications());
            queueMetric.setContainersUsed(capacitySchedulerLeafQueueInfo.getNumContainers());
            queueMetric.setMemoryUsed((int) capacitySchedulerLeafQueueInfo.getResourcesUsed().getMemorySize());
            queueMetric.setPendingApplications(capacitySchedulerLeafQueueInfo.getNumPendingApplications());
            queueMetric.setvCoresUsed(capacitySchedulerLeafQueueInfo.getResourcesUsed().getvCores());
            QueueMeasurement queueMeasurement = new QueueMeasurement(queueMetric);
            metricService.write(MetricDB.LEYARN, queueMeasurement);
        }
    }

    private Map<String, Object> generateCompletedJobFieldMap(ApplicationReport appReport) {
        Map<String, Object> fieldMap = new HashMap<>();
        ApplicationResourceUsageReport resourceReport = appReport.getApplicationResourceUsageReport();
        Resource usedResources = resourceReport.getUsedResources();
        fieldMap.put("AllocatedMB", usedResources.getMemory());
        fieldMap.put("AllocatedVCores", usedResources.getVirtualCores());
        fieldMap.put("MemorySec", (int) resourceReport.getMemorySeconds());
        fieldMap.put("VcoreSec", (int) resourceReport.getVcoreSeconds());
        fieldMap.put("ElapsedTimeSec",
                Integer.valueOf((int) Times.elapsed(appReport.getStartTime(), appReport.getFinishTime()) / 1000));

        Job job = null;
        try {
            job = workflowProxy.getWorkflowJobFromApplicationId(appReport.getApplicationId().toString());
        } catch (Exception e) {
            // do nothing
        }
        if (job != null && job.getSteps() != null) {
            for (JobStep step : job.getSteps()) {
                if (step.getEndTimestamp() == null || step.getStartTimestamp() == null) {
                    fieldMap.put(step.getJobStepType() + "Sec", 0);
                    continue;
                }
                long elapsedMillis = step.getEndTimestamp().getTime() - step.getStartTimestamp().getTime();
                int elapsedSec = 0;
                if (elapsedMillis > 0) {
                    elapsedSec = (int) elapsedMillis / 1000;
                } else {
                    elapsedSec = 0;
                }
                fieldMap.put(step.getJobStepType() + "Sec", elapsedSec);
            }
            List<Report> reports = job.getReports();
            reports.stream().filter(r -> reportsPurposes.contains(r.getPurpose()))
                    .map(r -> JsonUtils.deserialize(r.getJson().getPayload(), new TypeReference<Map<String, Object>>() {
                    })).filter(Objects::nonNull).forEach(fieldMap::putAll);
        }
        return fieldMap;
    }

}
