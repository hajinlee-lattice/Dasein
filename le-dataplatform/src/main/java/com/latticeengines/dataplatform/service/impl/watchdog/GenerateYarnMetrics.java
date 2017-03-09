package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.latticeengines.dataplatform.exposed.entitymanager.YarnMetricGeneratorInfoEntityMgr;
import com.latticeengines.dataplatform.runtime.metric.CompletedJobByAppIdMeasurement;
import com.latticeengines.dataplatform.runtime.metric.CompletedJobByAppIdMetric;
import com.latticeengines.dataplatform.runtime.metric.CompletedJobMeasurement;
import com.latticeengines.dataplatform.runtime.metric.CompletedJobMetric;
import com.latticeengines.dataplatform.runtime.metric.InProgressJobMeasurement;
import com.latticeengines.dataplatform.runtime.metric.InProgressJobMetric;
import com.latticeengines.dataplatform.runtime.metric.QueueMeasurement;
import com.latticeengines.dataplatform.runtime.metric.QueueMetric;
import com.latticeengines.dataplatform.service.impl.JobNameServiceImpl;
import com.latticeengines.domain.exposed.dataplatform.metrics.YarnMetricGeneratorInfo;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("generateYarnMetrics")
public class GenerateYarnMetrics extends WatchdogPlugin {

    private static final Log log = LogFactory.getLog(GenerateYarnMetrics.class);

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
            "propdataTransformationWorkflow", //
            "publishWorkflow", //
            "rtsBulkScoreWorkflow", //
            "scoreWorkflow");

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
    public void run(JobExecutionContext context) throws JobExecutionException {
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
        AppsInfo appsInfo = getYarnService().getApplications(query);
        List<AppInfo> apps = appsInfo.getApps();
        log.info(String.format("Processing %d apps with %s status", apps.size(), finalAppStatus));

        long finishTime = finishedTimeBegin;
        for (AppInfo app : apps) {
            // Evaluate all (not just the workflow-driver) apps for the
            // finishTime
            finishTime = Math.max(finishTime, app.getFinishTime());

            List<String> splits = nameSplitter.splitToList(app.getName());
            String suffix = splits.get(splits.size() - 1);
            if (!workflowJobNames.contains(suffix)) {
                // skip non-workflow-driver jobs
                continue;
            }

            Map<String, Object> fieldMap = generateCompletedJobFieldMap(app);

            CompletedJobMetric completedJobMetric = new CompletedJobMetric();
            completedJobMetric.setQueue(app.getQueue());
            completedJobMetric.setFinalAppStatus(FinalApplicationStatus.SUCCEEDED.name());
            completedJobMetric.setJobName(suffix);
            completedJobMetric.setTenantId(splits.get(0));
            CompletedJobMeasurement completedJobMeasurement = new CompletedJobMeasurement(completedJobMetric);
            metricService.write(MetricDB.LEYARN, completedJobMeasurement, fieldMap);

            CompletedJobByAppIdMetric completedJobByAppIdMetric = new CompletedJobByAppIdMetric();
            completedJobByAppIdMetric.setApplicationId(app.getAppId());
            CompletedJobByAppIdMeasurement completedJobByAppIdMeasurement = new CompletedJobByAppIdMeasurement(
                    completedJobByAppIdMetric);
            metricService.write(MetricDB.LEYARN, completedJobByAppIdMeasurement, fieldMap);
        }
        return finishTime;
    }

    private void generateInProgressJobMetrics() {
        AppsInfo runningAppsInfo = getYarnService().getApplications("finalStatus=" + FinalApplicationStatus.UNDEFINED);

        List<AppInfo> apps = runningAppsInfo.getApps();

        log.info(String.format("Processing %d apps in progress", apps.size()));
        for (AppInfo app : apps) {
            List<String> splits = nameSplitter.splitToList(app.getName());
            String suffix = splits.get(splits.size() - 1);
            InProgressJobMetric jobMetric = new InProgressJobMetric();
            jobMetric.setApplicationId(app.getAppId());
            jobMetric.setQueue(app.getQueue());
            jobMetric.setAllocatedMB(app.getAllocatedMB());
            jobMetric.setAllocatedVCores(app.getAllocatedVCores());
            jobMetric.setMemorySeconds((int) app.getMemorySeconds());
            jobMetric.setRunningContainers(app.getRunningContainers());
            jobMetric.setVcoreSeconds((int) app.getVcoreSeconds());
            jobMetric.setElapsedTimeSec(Integer.valueOf((int) app.getElapsedTime() / 1000));

            jobMetric.setFinalAppStatus(FinalApplicationStatus.UNDEFINED.name());

            String split = splits.get(0);
            if (!split.contains("[") && !split.contains("]")
                    && (split.endsWith(".Production") || split.toLowerCase().endsWith("playmaker"))) {
                jobMetric.setTenantId(split);
            } else {
                log.info("Job does not contain proper tenant name:" + split + " split from: " + app.getName());
                jobMetric.setTenantId("NA");
            }

            if (workflowJobNames.contains(suffix)) {
                jobMetric.setIsWorkflowDriver(true);
                jobMetric.setJobName(suffix);
            } else {
                jobMetric.setIsWorkflowDriver(false);
                jobMetric.setJobName("NA");
            }

            if (app.getState().equalsIgnoreCase(YarnApplicationState.RUNNING.name())) {
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
            queueMetric.setMemoryUsed(capacitySchedulerLeafQueueInfo.getResourcesUsed().getMemory());
            queueMetric.setPendingApplications(capacitySchedulerLeafQueueInfo.getNumPendingApplications());
            queueMetric.setvCoresUsed(capacitySchedulerLeafQueueInfo.getResourcesUsed().getvCores());
            QueueMeasurement queueMeasurement = new QueueMeasurement(queueMetric);
            metricService.write(MetricDB.LEYARN, queueMeasurement);
        }
    }

    private Map<String, Object> generateCompletedJobFieldMap(AppInfo app) {
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("AllocatedMB", app.getAllocatedMB());
        fieldMap.put("AllocatedVCores", app.getAllocatedVCores());
        fieldMap.put("MemorySec", app.getMemorySeconds());
        fieldMap.put("VcoreSec", app.getVcoreSeconds());
        fieldMap.put("ElapsedTimeSec", Integer.valueOf((int) app.getElapsedTime() / 1000));

        Job job = null;
        try {
            job = workflowProxy.getWorkflowJobFromApplicationId(app.getAppId());
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
        }

        return fieldMap;
    }

}
