package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingStatus;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class WorkflowJobServiceImplUnitTestNG {

    @Mock
    private WorkflowProxy workflowProxy;

    @Mock
    private SourceFileProxy sourceFileProxy;

    @Mock
    private TenantEntityMgr tenantEntityMgr;

    @Mock
    private ActionProxy actionProxy;

    @Mock
    private BatonService batonService;

    @Mock
    private DataFeedProxy dataFeedProxy;

    @InjectMocks
    private WorkflowJobServiceImpl workflowJobService;

    @Mock
    private RatingEngineProxy ratingEngineProxy;

    @Mock
    private ModelSummaryProxy modelSummaryProxy;

    @Mock
    private CDLProxy cdlProxy;

    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImplUnitTestNG.class);

    private Long[] jobIds = { 123L, 456L };
    private Long TS_2019_01_21 = 1548028800000L;
    private Long TS_2019_01_20 = 1547942400000L;
    private Long TS_2019_01_19 = 1547856000000L;
    private Long TS_2019_01_18 = 1547769600000L;
    private Long[] actionPids = { 101L, 102L, 103L };

    private static final String INITIATOR = "test@lattice-engines.com";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockWorkflowProxy();
        mockSourceFileEntityManager();
        mockTenantEntityManager();
        mockActionService();
        mockDataFeedProxy();
        mockModelSummaryProxy();
        mockCdlProxy();

        Tenant tenant = tenantEntityMgr.findByTenantId("tenant");
        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "unit")
    public void testFind() {
        Job job = workflowJobService.find("1001", true);
        assertEquals(job.getId(), jobIds[0]);
        assertNotNull(job.getInputs());
        assertEquals(job.getJobStatus(), JobStatus.RUNNING);
        assertEquals(job.getJobType(), "importMatchAndModelWorkflow");
        assertEquals(job.getName(), "importMatchAndModelWorkflow");
        assertEquals(job.getDescription(), "importMatchAndModelWorkflow");
        List<JobStep> steps = job.getSteps();
        assertEquals(steps.size(), 3);
        for (JobStep step : steps) {
            if (step.getJobStepType().equalsIgnoreCase("importdata")) {
                assertEquals(step.getName(), "load_data");
                assertEquals(step.getDescription(), "load_data");
            } else if (step.getJobStepType().equalsIgnoreCase("createeventtablereport")) {
                assertEquals(step.getName(), "load_data");
                assertEquals(step.getDescription(), "load_data");
            } else if (step.getJobStepType().equalsIgnoreCase("createprematcheventtablereport")) {
                assertEquals(step.getName(), "generate_insights");
                assertEquals(step.getDescription(), "generate_insights");
            }
        }
        assertEquals(job.getNumDisplayedSteps().intValue(), 2);
    }

    @Test(groups = "unit")
    public void testFindByJobIds() {
        List<String> jobIdStrs = Arrays.stream(jobIds).map(String::valueOf).collect(Collectors.toList());
        log.info(String.format("jobIdStrs are %s", jobIdStrs));
        List<Job> jobs = workflowJobService.findByJobIds(jobIdStrs, true, false);
        assertNotNull(jobs);
        assertEquals(jobs.size(), jobIds.length);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "unit", dependsOnMethods = { "testExpandActions" })
    public void testGenerateUnstartedProcessAnalyzeJob() {
        // Auto Schedule is off
        when(batonService.isEnabled(any(CustomerSpace.class), any(LatticeFeatureFlag.class))).thenReturn(false);
        Job job = workflowJobService.generateUnstartedProcessAnalyzeJob(false, null);
        testUnstartedPnAJob(job);
        Assert.assertNotNull(job.getInputs());
        Assert.assertNotNull(job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_IDS));
        List<Object> listObj = JsonUtils.deserialize(job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_IDS),
                List.class);
        Assert.assertEquals(listObj.size(), 5);
        Assert.assertNull(job.getNote());
        log.info(String.format("listObj is %s", listObj));

        // Auto Schedule is on
        when(batonService.isEnabled(any(CustomerSpace.class), any(LatticeFeatureFlag.class))).thenReturn(true);
        job = workflowJobService.generateUnstartedProcessAnalyzeJob(true, null);
        Assert.assertNotNull(job.getSubJobs());
        Assert.assertEquals(job.getSubJobs().size(), 4);
        Assert.assertNotNull(job.getNote());
        log.info("Note is " + job.getNote());

        // test when no action, we still have P&A job
        when(actionProxy.getActionsByOwnerId(anyString(), isNull())).thenReturn(Collections.EMPTY_LIST);
        job = workflowJobService.generateUnstartedProcessAnalyzeJob(false, null);
        testUnstartedPnAJob(job);
    }

    private void testUnstartedPnAJob(Job job) {
        Date now = new DateTime().toDate();
        Assert.assertNotNull(job);
        Assert.assertEquals(job.getName(), "processAnalyzeWorkflow");
        Assert.assertEquals(job.getJobType(), "processAnalyzeWorkflow");
        Assert.assertEquals(job.getJobStatus(), JobStatus.READY);
        Assert.assertEquals(job.getId(), WorkflowJobServiceImpl.UNSTARTED_PROCESS_ANALYZE_ID);
        Assert.assertTrue(job.getStartTimestamp().after(now));
    }

    @Test(groups = "unit", dependsOnMethods = { "testGenerateUnstartedProcessAnalyzeJob" })
    public void updateStartTimeStampAndForJob() {
        DateTime nextInvokeDate = new DateTime().plusDays(1).withTimeAtStartOfDay();

        Job job = new Job();
        when(batonService.isEnabled(any(CustomerSpace.class), any(LatticeFeatureFlag.class))).thenReturn(true);
        DateTime now = new DateTime();
        log.info("now = " + now.toDate());
        when(dataFeedProxy.nextInvokeTime(anyString())).thenReturn(null);
        workflowJobService.updateStartTimeStampAndForJob(job);
        log.info("job.startDate = " + job.getStartTimestamp());
        Assert.assertTrue(nextInvokeDate.isEqual(job.getStartTimestamp().getTime()));

        long previous25hour = new DateTime().minusHours(25).toDate().getTime();
        when(dataFeedProxy.nextInvokeTime(anyString())).thenReturn(previous25hour);
        log.info("hour of day is " + new DateTime(previous25hour).getHourOfDay());
        workflowJobService.updateStartTimeStampAndForJob(job);
        log.info("job.startDate = " + job.getStartTimestamp());
        Assert.assertTrue(now.isBefore(job.getStartTimestamp().getTime()));
        Assert.assertEquals(new DateTime(job.getStartTimestamp().getTime()).getHourOfDay(),
                new DateTime(previous25hour).getHourOfDay());
        Assert.assertEquals(new DateTime(job.getStartTimestamp().getTime()).getMinuteOfHour(),
                new DateTime(previous25hour).getMinuteOfHour());
        Assert.assertEquals(new DateTime(job.getStartTimestamp().getTime()).getSecondOfMinute(),
                new DateTime(previous25hour).getSecondOfMinute());

        when(dataFeedProxy.nextInvokeTime(anyString())).thenReturn(new DateTime().plusHours(1).toDate().getTime());
        workflowJobService.updateStartTimeStampAndForJob(job);
        now = new DateTime();
        DateTime next2hours = new DateTime().plusHours(2);
        log.info("job.startDate = " + job.getStartTimestamp());
        Assert.assertTrue(now.isBefore(job.getStartTimestamp().getTime())
                && next2hours.isAfter(job.getStartTimestamp().getTime()));

    }

    @Test(groups = "unit")
    public void testGetActionIdsForJob() {
        List<Long> actionIds = workflowJobService.getActionIdsForJob(createProcessAnalyzeJob(jobIds[0], actionPids));
        Assert.assertEquals(actionIds.size(), 3);
        Assert.assertTrue(actionIds.contains(101L) && actionIds.contains(102L) && actionIds.contains(103L));
        log.info(String.format("actionIds=%s", actionIds));
    }

    private void verifyActionAndJobsForExpandActions(List<Action> actions, int actionSize, List<Job> expandedJobs) {
        Assert.assertEquals(actions.size(), actionSize);
        Assert.assertEquals(expandedJobs.size(), 4);
        Job firstJob = expandedJobs.get(0);
        Assert.assertEquals(firstJob.getName(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getDisplayName());
        Assert.assertEquals(firstJob.getJobType(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getName());
        Assert.assertEquals(firstJob.getUser(), INITIATOR);
        Assert.assertEquals(firstJob.getJobStatus(), JobStatus.RUNNING);

        Job secondJob = expandedJobs.get(1);
        Assert.assertEquals(secondJob.getName(), ActionType.METADATA_CHANGE.getDisplayName());
        Assert.assertEquals(secondJob.getJobType(), ActionType.METADATA_CHANGE.getName());
        Assert.assertEquals(secondJob.getUser(), INITIATOR);
        Assert.assertEquals(secondJob.getJobStatus(), JobStatus.COMPLETED);
        log.info(String.format("expandedJobs=%s", expandedJobs));
    }

    @Test(groups = "unit")
    public void testExpandActions() {
        List<Action> actions = generateActions();
        List<Job> expandedJobs = workflowJobService.expandActions(actions, null);
        verifyActionAndJobsForExpandActions(actions, 6, expandedJobs);
        actions = generateActions();
        List<Job> jobs = new ArrayList<>();
        jobs.add(createJob(jobIds[0]));
        jobs.add(createJob(jobIds[1]));
        Map<String, Job> jobMap = jobs.stream().filter(job -> job.getPid() != null)
                .collect(Collectors.toMap(job -> job.getPid().toString(), Job -> Job, (key1, key2) -> key2));
        actions.remove(5);
        expandedJobs = workflowJobService.expandActions(actions, jobMap);
        verifyActionAndJobsForExpandActions(actions, 5, expandedJobs);
    }

    @Test(groups = "unit")
    public void testFindJobsBasedOnActionIdsAndType() {
        List<Job> jobs = workflowJobService.findJobsBasedOnActionIdsAndType(Arrays.asList(1L, 2L, 3L, 4L),
                ActionType.CDL_OPERATION_WORKFLOW);
        Assert.assertTrue(CollectionUtils.isEmpty(jobs));
        jobs = workflowJobService.findJobsBasedOnActionIdsAndType(Arrays.asList(1L, 2L, 3L, 4L),
                ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Assert.assertFalse(CollectionUtils.isEmpty(jobs));
        Assert.assertEquals(jobs.size(), 3);
        jobs = workflowJobService.findJobsBasedOnActionIdsAndType(null, null);
        Assert.assertNotNull(jobs);
        Assert.assertEquals(jobs.size(), 0);
    }

    private List<Action> generateActionWithJobIdAndActionPids(Long jobId, Long[] actionPids) {
        List<Action> actions = new ArrayList<>();
        Action action1 = new Action();
        action1.setPid(actionPids[0]);
        action1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action1.setActionConfiguration(new ImportActionConfiguration());
        action1.setActionInitiator(INITIATOR);
        action1.setTrackingPid(jobId);
        Action action2 = new Action();
        action2.setPid(actionPids[1]);
        action2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action2.setActionConfiguration(new ImportActionConfiguration());
        action2.setTrackingPid(jobId);
        Action action3 = new Action();
        action3.setPid(actionPids[2]);
        action3.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action3.setActionConfiguration(new ImportActionConfiguration());
        action3.setActionStatus(ActionStatus.CANCELED);
        action3.setTrackingPid(jobId);
        actions.add(action1);
        actions.add(action2);
        actions.add(action3);
        return actions;
    }

    @Test(groups = "unit")
    public void testUpdateJobWithSubJobsIfIsPnA() {
        Job job1 = createProcessAnalyzeJob(jobIds[0], actionPids);
        workflowJobService.updateJobWithSubJobsIfIsPnA(job1, null, null, null);
        Assert.assertNotNull(job1.getSubJobs());
        job1 = createProcessAnalyzeJob(jobIds[0], actionPids);
        job1.setJobType("bulkmatchworkflow");
        workflowJobService.updateJobWithSubJobsIfIsPnA(job1, null, null, null);
        Assert.assertNull(job1.getSubJobs());

        Map<String, Job> jobMap = new HashMap<>();
        job1 = createProcessAnalyzeJob(jobIds[0], actionPids);
        jobMap.put(job1.getPid().toString(), job1);
        List<Action> actions = generateActionWithJobIdAndActionPids(jobIds[0], actionPids);
        Map<Long, Action> actionIdMap;
        Map<Long, Action> actionJobPidMap;
        actionIdMap = actions.stream().filter(action -> action.getPid() != null)
                .collect(Collectors.toMap(Action::getPid, Action -> Action, (key1, key2) -> key2));
        actionJobPidMap = actions.stream().filter(action -> action.getTrackingPid() != null)
                .collect(Collectors.toMap(Action::getTrackingPid, Action -> Action, (key1, key2) -> key2));
        workflowJobService.updateJobWithSubJobsIfIsPnA(job1, jobMap, actionIdMap, actionJobPidMap);
        Assert.assertNotNull(job1.getSubJobs());
        job1 = createProcessAnalyzeJob(jobIds[0], actionPids);
        job1.setJobType("bulkmatchworkflow");
        jobMap = new HashMap<>();
        jobMap.put(job1.getPid().toString(), job1);
        actions = generateActionWithJobIdAndActionPids(jobIds[0], actionPids);
        actionIdMap = actions.stream().filter(action -> action.getPid() != null)
                .collect(Collectors.toMap(Action::getPid, Action -> Action, (key1, key2) -> key2));
        actionJobPidMap = actions.stream().filter(action -> action.getTrackingPid() != null)
                .collect(Collectors.toMap(Action::getTrackingPid, Action -> Action, (key1, key2) -> key2));
        workflowJobService.updateJobWithSubJobsIfIsPnA(job1, jobMap, actionIdMap, actionJobPidMap);
        Assert.assertNull(job1.getSubJobs());
    }

    @Test(groups = "unit")
    public void testUpdateJobActionId() {
        Job job1 = createFailedImportJob(jobIds[1]);
        Job job2 = createFailedImportJob(jobIds[0]);
        List<Job> jobList = new ArrayList<>();
        jobList.add(job1);
        jobList.add(job2);
        Assert.assertNull(job1.getInputs().get(WorkflowContextConstants.Inputs.ACTION_ID));
        Assert.assertNull(job2.getInputs().get(WorkflowContextConstants.Inputs.ACTION_ID));
        List<Action> actions = trackingActions();
        when(actionProxy.getActionsByJobPids(anyString(), any())).thenReturn(actions);
        workflowJobService.updateJobsWithActionId(jobList, null);
        log.info(" jobs is :" + JsonUtils.serialize(jobList));
        Assert.assertNotNull(jobList.get(0).getInputs().get(WorkflowContextConstants.Inputs.ACTION_ID));
        Assert.assertNotNull(jobList.get(1).getInputs().get(WorkflowContextConstants.Inputs.ACTION_ID));

        Map<Long, Action> actionJobPidMap = actions.stream().filter(action -> action.getTrackingPid() != null)
                .collect(Collectors.toMap(Action::getTrackingPid, Action -> Action, (key1, key2) -> key2));
        workflowJobService.updateJobsWithActionId(jobList, actionJobPidMap);
        jobList = new ArrayList<>();
        jobList.add(job1);
        jobList.add(job2);
        Assert.assertNotNull(jobList.get(0).getInputs().get(WorkflowContextConstants.Inputs.ACTION_ID));
        Assert.assertNotNull(jobList.get(1).getInputs().get(WorkflowContextConstants.Inputs.ACTION_ID));
    }

    @Test(groups = "unit")
    public void testUpdateOrphanJobsByPA() {
        List<Job> jobList = prepareJobList();
        workflowJobService.updateExpiredOrphanJobs(jobList);
        jobList.forEach(job -> {
            if (job.getJobType().equals(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE)) {
                Assert.assertEquals(job.getInputs().get("EXPORT_ID"), WorkflowJobServiceImpl.ORPHAN_ARTIFACT_EXPIRED);
            }
        });
    }

    @Test(groups = "unit")
    public void testUpdateOrphanJobsWithoutPA() {
        List<Job> jobList = prepareJobList();
        jobList.removeIf(job -> job.getJobType().equals(WorkflowJobServiceImpl.PA_JOB_TYPE));
        jobList.removeIf(job -> job.getJobType().equals(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE)
                && job.getStartTimestamp().compareTo(new Date(TS_2019_01_20)) == 0);
        workflowJobService.updateExpiredOrphanJobs(jobList);
        jobList.forEach(job -> {
            if (job.getJobType().equals(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE)) {
                Assert.assertNotEquals(job.getInputs().get("EXPORT_ID"),
                        WorkflowJobServiceImpl.ORPHAN_ARTIFACT_EXPIRED);
            }
        });
    }

    private List<Job> getJobList(String jobType) {
        Long[] actionPids = { 1L, 2L, 3L, 4L, 5L };
        Job job1 = createProcessAnalyzeJob(jobIds[0], actionPids);
        job1.setJobType(jobType);
        List<Job> jobs = new ArrayList<>();
        jobs.add(job1);
        return jobs;
    }

    @Test(groups = "unit")
    public void testUpdateAllJobs() {
        List<Job> jobs = getJobList("processAnalyzeWorkflow");
        workflowJobService.updateAllJobs(jobs);
        Job job1 = jobs.get(0);
        Assert.assertNotNull(job1.getSubJobs());
        Assert.assertEquals(job1.getSubJobs().size(), 4);
        jobs = getJobList("bulkmatchworkflow");
        job1 = jobs.get(0);
        workflowJobService.updateAllJobs(jobs);
        Assert.assertNull(job1.getSubJobs());
    }

    @Test(groups = "unit")
    public void testGetActionPids() {
        List<Job> jobs = getJobList("processAnalyzeWorkflow");
        List<Long> actionPids = workflowJobService.getActionPids(jobs);
        Assert.assertEquals(actionPids.size(), 5);
        Assert.assertTrue(actionPids.containsAll(Arrays.asList(1L, 2L, 3L, 4L, 5L)));
        jobs = getJobList("bulkmatchworkflow");
        actionPids = workflowJobService.getActionPids(jobs);
        Assert.assertEquals(actionPids.size(), 0);
    }

    @Test(groups = "unit")
    public void testGetJobMap() {
        List<Action> actions = generateActions();
        Map<Long, Action> actionJobPidMap = actions.stream().filter(action -> action.getTrackingPid() != null)
                .collect(Collectors.toMap(Action::getTrackingPid, Action -> Action, (key1, key2) -> key2));
        Map<String, Job> jobMap = workflowJobService.getJobMap(actionJobPidMap);
        Assert.assertEquals(jobMap.size(), 2);
    }

    @Test(groups = "unit")
    public void testGetActions() {
        List<Long> actionIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        List<Action> actions = generateActions();
        Map<Long, Action> actionIdMap = actions.stream().filter(action -> action.getPid() != null)
                .collect(Collectors.toMap(Action::getPid, Action -> Action, (key1, key2) -> key2));
        List<Action> actions2 = workflowJobService.getActions(actionIds, actionIdMap);
        Assert.assertEquals(actions2.size(), 5);
    }

    @Test(groups = "unit")
    public void testUpdateOrphanJobsBySameOrphanType() {
        List<Job> jobList = prepareJobList();
        Job paJob = jobList.get(1);
        paJob.setStartTimestamp(new Date(TS_2019_01_19));
        jobList.set(1, paJob);
        workflowJobService.updateExpiredOrphanJobs(jobList);
        Assert.assertEquals(jobList.stream()
                .filter(job -> job.getJobType().equals(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE)
                        && job.getInputs().get("ARTIFACT_TYPE").equals(OrphanRecordsType.TRANSACTION.getOrphanType())
                        && job.getInputs().get("EXPORT_ID").equals(WorkflowJobServiceImpl.ORPHAN_ARTIFACT_EXPIRED))
                .count(), 1);
        Assert.assertEquals(jobList.stream()
                .filter(job -> job.getJobType().equals(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE) && job.getInputs()
                        .get("ARTIFACT_TYPE").equals(OrphanRecordsType.CONTACT.getOrphanType())
                        && job.getInputs().get("EXPORT_ID").equals(WorkflowJobServiceImpl.ORPHAN_ARTIFACT_EXPIRED))
                .count(), 1);
        Assert.assertEquals(jobList.stream()
                .filter(job -> job.getJobType().equals(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE)
                        && job.getInputs().get("ARTIFACT_TYPE")
                                .equals(OrphanRecordsType.UNMATCHED_ACCOUNT.getOrphanType())
                        && job.getInputs().get("EXPORT_ID").equals(WorkflowJobServiceImpl.ORPHAN_ARTIFACT_EXPIRED))
                .count(), 1);
    }

    private List<Job> prepareJobList() {
        List<Job> jobList = new ArrayList<>();
        Job pa1 = createProcessAnalyzeJob(1L, actionPids);
        Job pa2 = createProcessAnalyzeJob(2L, actionPids);
        pa1.setStartTimestamp(new Date(TS_2019_01_18));
        pa2.setStartTimestamp(new Date(TS_2019_01_21));
        jobList.add(pa1);
        jobList.add(pa2);

        Job trx1 = createOrphanJob(TS_2019_01_19, OrphanRecordsType.TRANSACTION);
        Job trx2 = createOrphanJob(TS_2019_01_20, OrphanRecordsType.TRANSACTION);
        jobList.add(trx1);
        jobList.add(trx2);

        Job contact1 = createOrphanJob(TS_2019_01_19, OrphanRecordsType.CONTACT);
        Job contact2 = createOrphanJob(TS_2019_01_20, OrphanRecordsType.CONTACT);
        jobList.add(contact1);
        jobList.add(contact2);

        Job acct1 = createOrphanJob(TS_2019_01_19, OrphanRecordsType.UNMATCHED_ACCOUNT);
        Job acct2 = createOrphanJob(TS_2019_01_20, OrphanRecordsType.UNMATCHED_ACCOUNT);
        jobList.add(acct1);
        jobList.add(acct2);

        return jobList;
    }

    private Job createOrphanJob(Long startTimestamp, OrphanRecordsType artifactType) {
        Job job = new Job();
        job.setId(new Random().nextLong());
        job.setName(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE);
        job.setDescription(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE);
        job.setJobType(WorkflowJobServiceImpl.ORPHAN_JOB_TYPE);
        job.setStartTimestamp(new Date(startTimestamp));
        job.setJobStatus(JobStatus.COMPLETED);
        Map<String, String> inputContext = new HashMap<>();
        inputContext.put("ARTIFACT_TYPE", artifactType.getOrphanType());
        inputContext.put("EXPORT_ID", UUID.randomUUID().toString());
        job.setInputs(inputContext);
        return job;
    }

    private void mockWorkflowProxy() {
        when(workflowProxy.getWorkflowExecution(anyString(), anyString())).thenReturn(createJob(jobIds[0]));
        when(workflowProxy.getWorkflowExecution(anyString())).thenReturn(createJob(jobIds[0]));
        List<Job> jobs = new ArrayList<>();
        jobs.add(createJob(jobIds[0]));
        jobs.add(createJob(jobIds[1]));
        when(workflowProxy.getWorkflowExecutionsByJobPids(anyList(), anyString())).thenReturn(jobs);
        when(workflowProxy.getWorkflowExecutionsByJobIds(anyList(), anyString())).thenReturn(jobs);
        when(workflowProxy.getWorkflowExecutionsByJobPids(anyList(), new String[] { anyString() })).thenReturn(jobs);
    }

    private void mockSourceFileEntityManager() {
        when(sourceFileProxy.findByApplicationId(any(), anyString())).thenReturn(new SourceFile());
    }

    private void mockModelSummaryProxy() {
        when(modelSummaryProxy.getByModelId(anyString())).thenReturn(new ModelSummary());
    }

    private void mockCdlProxy() {
        when(cdlProxy.getSchedulingStatus(anyString())).thenReturn(new SchedulingStatus("", false, null, null));
    }

    private void mockTenantEntityManager() {
        Tenant tenant = new Tenant();
        tenant.setId("tenant");
        tenant.setPid(1L);
        when(tenantEntityMgr.findByTenantId(anyString())).thenReturn(tenant);
    }

    private void mockActionService() {
        when(actionProxy.getActionsByOwnerId(anyString(), isNull())).thenReturn(generateActions());
        when(actionProxy.getActionsByPids(anyString(), anyList())).thenReturn(generateActions());
    }

    private void mockDataFeedProxy() {
        when(dataFeedProxy.nextInvokeTime(anyString())).thenReturn(new DateTime().plusDays(1).toDate().getTime());
    }

    private List<Action> generateActions() {
        List<Action> actions = new ArrayList<>();
        Action action1 = new Action();
        action1.setPid(1L);
        action1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action1.setActionConfiguration(new ImportActionConfiguration());
        action1.setActionInitiator(INITIATOR);
        Action action2 = new Action();
        action2.setPid(2L);
        action2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action2.setActionConfiguration(new ImportActionConfiguration());
        action2.setTrackingPid(jobIds[0]);
        Action action3 = new Action();
        action3.setPid(3L);
        action3.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action3.setActionConfiguration(new ImportActionConfiguration());
        action3.setActionStatus(ActionStatus.CANCELED);
        action3.setTrackingPid(jobIds[1]);
        Action action4 = new Action();
        action4.setPid(4L);
        action4.setType(ActionType.METADATA_CHANGE);
        action4.setActionConfiguration(new RatingEngineActionConfiguration());
        action4.setActionInitiator(INITIATOR);
        Action action5 = new Action();
        action5.setPid(5L);
        action5.setType(ActionType.RATING_ENGINE_CHANGE);
        ActionConfiguration actionConfig = new RatingEngineActionConfiguration();
        actionConfig.setHiddenFromUI(true);
        action5.setActionInitiator(INITIATOR);
        action5.setActionConfiguration(actionConfig);
        Action action6 = new Action();
        action6.setPid(3L);
        action6.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action6.setActionConfiguration(new ImportActionConfiguration());
        action6.setActionStatus(ActionStatus.ACTIVE);
        action6.setTrackingPid(jobIds[1]);
        actions.add(action1);
        actions.add(action2);
        actions.add(action3);
        actions.add(action4);
        actions.add(action5);
        actions.add(action6);
        return actions;
    }

    private List<Action> trackingActions() {
        List<Action> actions = new ArrayList<>();
        Action action1 = new Action();
        action1.setPid(2L);
        action1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action1.setActionConfiguration(new ImportActionConfiguration());
        action1.setTrackingPid(jobIds[0]);
        Action action2 = new Action();
        action2.setPid(3L);
        action2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action2.setActionConfiguration(new ImportActionConfiguration());
        action2.setActionStatus(ActionStatus.CANCELED);
        action2.setTrackingPid(jobIds[1]);
        Action action3 = new Action();
        action3.setPid(3L);
        action3.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action3.setActionConfiguration(new ImportActionConfiguration());
        action3.setActionStatus(ActionStatus.ACTIVE);
        action3.setTrackingPid(jobIds[1]);
        actions.add(action1);
        actions.add(action2);
        actions.add(action3);
        return actions;
    }

    private Job createJob(Long jobId) {
        JobStep stepImportData = new JobStep();
        stepImportData.setJobStepType("importdata");
        stepImportData.setStepStatus(JobStatus.COMPLETED);

        JobStep stepCreateEventTableReport = new JobStep();
        stepCreateEventTableReport.setJobStepType("createeventtablereport");
        stepCreateEventTableReport.setStepStatus(JobStatus.RUNNING);

        JobStep stepCreatePrematchEventTableReport = new JobStep();
        stepCreatePrematchEventTableReport.setJobStepType("createprematcheventtablereport");
        stepCreatePrematchEventTableReport.setStepStatus(JobStatus.PENDING);

        List<JobStep> steps = new ArrayList<>();
        steps.add(stepImportData);
        steps.add(stepCreateEventTableReport);
        steps.add(stepCreatePrematchEventTableReport);

        Job job = new Job();
        job.setId(jobId);
        job.setPid(jobId);
        job.setName("importMatchAndModelWorkflow");
        job.setDescription("importMatchAndModelWorkflow");
        job.setJobType("importMatchAndModelWorkflow");
        job.setStartTimestamp(new Date());
        job.setJobStatus(JobStatus.RUNNING);
        job.setSteps(steps);

        return job;
    }

    private Job createProcessAnalyzeJob(Long jobId, Long[] actionPids) {
        Job job = new Job();
        job.setId(jobId);
        job.setPid(jobId);
        job.setName("processAnalyzeWorkflow");
        job.setDescription("processAnalyzeWorkflow");
        job.setJobType("processAnalyzeWorkflow");
        job.setStartTimestamp(new Date());
        job.setJobStatus(JobStatus.COMPLETED);
        List<Long> actionIds = Arrays.asList(actionPids);
        Map<String, String> inputContext = new HashMap<>();
        inputContext.put(WorkflowContextConstants.Inputs.ACTION_IDS, actionIds.toString());
        job.setInputs(inputContext);
        return job;
    }

    private Job createFailedImportJob(Long jobId) {
        Job job = new Job();
        job.setId(jobId);
        job.setPid(jobId);
        job.setName("cdlDataFeedImportWorkflow");
        job.setDescription("cdlDataFeedImportWorkflow");
        job.setJobType("cdlDataFeedImportWorkflow");
        job.setStartTimestamp(new Date());
        job.setJobStatus(JobStatus.FAILED);
        Map<String, String> inputContext = new HashMap<>();
        job.setInputs(inputContext);
        return job;
    }

    @Test(groups = "unit")
    public void testUpdateJobWithRatingEngine() {
        String ratingEngineId = "engine_hcnrj_a3qfsaty3puoih1q";
        String oldRatingEngineName = "oldName";
        // String newRatingEngineName = "newName";
        Job job = new Job();
        job.setJobType("customEventModelingWorkflow");
        Map<String, String> inputs = new HashMap<>();
        job.setInputs(inputs);
        inputs.put(WorkflowContextConstants.Inputs.RATING_ENGINE_ID, ratingEngineId);
        inputs.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, oldRatingEngineName);
        log.info("job is " + job);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngineId);
        ratingEngine.setDisplayName("newName");
        ratingEngine.setDescription("description");
        when(ratingEngineProxy.getRatingEngine(anyString(), anyString())).thenReturn(ratingEngine);
        workflowJobService.updateJobWithRatingEngine(job);
        Assert.assertEquals(job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME), "newName");
        log.info("new job is " + job);
    }
}
