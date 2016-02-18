package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceTestUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import com.latticeengines.monitor.alerts.service.impl.AlertServiceImpl;
import com.latticeengines.monitor.alerts.service.impl.PagerDutyTestUtils;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;

public class ModelCommandCallableMethodTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelingJobService modelingJobService;

    @Autowired
    private ModelStepYarnProcessor modelStepYarnProcessor;

    @Autowired
    private ModelCommandLogService modelCommandLogService;

    @Autowired
    private ModelStepProcessor modelStepFinishProcessor;

    @Autowired
    private ModelStepProcessor modelStepOutputResultsProcessor;

    @Autowired
    private ModelStepProcessor modelStepRetrieveMetadataProcessor;

    @Autowired
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Autowired
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Autowired
    private DebugProcessorImpl debugProcessorImpl;

    @Autowired
    private AlertService alertService;

    @Value("${dataplatform.yarn.resourcemanager.webapp.address}")
    private String resourceManagerWebAppAddress;

    @Value("${dataplatform.yarn.timeline-service.webapp.address}")
    private String appTimeLineWebAppAddress;

    @Value("${dataplatform.dlorchestrationjob.row.fail.threshold}")
    private int rowFailThreshold;

    @Value("${dataplatform.dlorchestrationjob.row.warn.threshold}")
    private int rowWarnThreshold;

    @Value("${dataplatform.dlorchestrationjob.postiveevent.fail.threshold}")
    private int positiveEventFailThreshold;

    @Value("${dataplatform.dlorchestrationjob.postiveevent.warn.threshold}")
    private int positiveEventWarnThreshold;

    @Autowired
    private MetadataService metadataService;

    @BeforeClass(groups = "functional")
    public void setup() {
        ((AlertServiceImpl) this.alertService).enableTestMode();
    }

    @Test(groups = "functional")
    public void testHandleJobFailed() throws ParseException {
        ModelCommand command = new ModelCommand(1L, "Nutanix", "Nutanix", ModelCommandStatus.NEW, null, ModelCommand.TAHOE,
                ModelingServiceTestUtils.EVENT_TABLE);
        command.setModelCommandStep(ModelCommandStep.PROFILE_DATA);
        this.modelCommandEntityMgr.create(command);

        this.modelCommandLogService.log(command, "message.  #%#$%%^$%^$%^$%^");
        this.modelCommandLogService.log(command, "another message.  #%#$%%^$%^$%^$%^ 12344       .");

        ModelCommandResult result = new ModelCommandResult(command, new Date(), new Date(),
                ModelCommandStatus.IN_PROGRESS);
        this.modelCommandResultEntityMgr.create(result);

        ModelCommandCallable.Builder builder = new ModelCommandCallable.Builder();
        builder.modelCommand(command) //
                .yarnConfiguration(this.yarnConfiguration) //
                .modelingJobService(this.modelingJobService) //
                .modelCommandEntityMgr(this.modelCommandEntityMgr) //
                .modelCommandStateEntityMgr(this.modelCommandStateEntityMgr) //
                .modelStepYarnProcessor(this.modelStepYarnProcessor) //
                .modelCommandLogService(this.modelCommandLogService) //
                .modelCommandResultEntityMgr(this.modelCommandResultEntityMgr) //
                .modelStepFinishProcessor(this.modelStepFinishProcessor) //
                .modelStepOutputResultsProcessor(this.modelStepOutputResultsProcessor) //
                .modelStepRetrieveMetadataProcessor(this.modelStepRetrieveMetadataProcessor) //
                .debugProcessorImpl(this.debugProcessorImpl) //
                .alertService(this.alertService) //
                .resourceManagerWebAppAddress(this.resourceManagerWebAppAddress) //
                .appTimeLineWebAppAddress(this.appTimeLineWebAppAddress) //
                .rowFailThreshold(this.rowFailThreshold) //
                .rowWarnThreshold(this.rowWarnThreshold) //
                .positiveEventFailThreshold(this.positiveEventFailThreshold) //
                .positiveEventWarnThreshold(this.positiveEventWarnThreshold) //
                .metadataService(this.metadataService);

        ModelCommandCallable callable = new ModelCommandCallable(builder);

        PagerDutyTestUtils.confirmPagerDutyIncident(callable.handleJobFailed());

        List<String> failedAppIds = new ArrayList<String>();
        failedAppIds.add("application_1415144508340_0729");
        PagerDutyTestUtils.confirmPagerDutyIncident(callable.handleJobFailed(failedAppIds));
    }

    @Test(groups = "functional")
    public void generateDataDiagnostics() throws Exception {
        ModelCommand command = new ModelCommand(2L, "Nutanix", "Nutanix", ModelCommandStatus.NEW, null, ModelCommand.TAHOE,
                ModelingServiceTestUtils.EVENT_TABLE);
        command.setModelCommandStep(ModelCommandStep.SUBMIT_MODELS);
        this.modelCommandEntityMgr.create(command);

        ModelCommandCallable.Builder builder = new ModelCommandCallable.Builder();
        builder.modelCommand(command) //
                .yarnConfiguration(this.yarnConfiguration) //
                .modelingJobService(this.modelingJobService) //
                .modelCommandEntityMgr(this.modelCommandEntityMgr) //
                .modelCommandStateEntityMgr(this.modelCommandStateEntityMgr) //
                .modelStepYarnProcessor(this.modelStepYarnProcessor) //
                .modelCommandLogService(this.modelCommandLogService) //
                .modelCommandResultEntityMgr(this.modelCommandResultEntityMgr) //
                .modelStepFinishProcessor(this.modelStepFinishProcessor) //
                .modelStepOutputResultsProcessor(this.modelStepOutputResultsProcessor) //
                .modelStepRetrieveMetadataProcessor(this.modelStepRetrieveMetadataProcessor) //
                .debugProcessorImpl(this.debugProcessorImpl) //
                .alertService(this.alertService) //
                .resourceManagerWebAppAddress(this.resourceManagerWebAppAddress) //
                .appTimeLineWebAppAddress(this.appTimeLineWebAppAddress) //
                .rowFailThreshold(this.rowFailThreshold) //
                .rowWarnThreshold(this.rowWarnThreshold) //
                .positiveEventFailThreshold(this.positiveEventFailThreshold) //
                .positiveEventWarnThreshold(this.positiveEventWarnThreshold) //
                .metadataService(this.metadataService);

        ModelCommandCallable callable = new ModelCommandCallable(builder);
        ModelCommandState commandState = new ModelCommandState(command, ModelCommandStep.SUBMIT_MODELS);
        JobStatus jobStatus = new JobStatus();

        String outputDir = "diagnostics_output";
        HdfsUtils.rmdir(this.yarnConfiguration, outputDir);
        HdfsUtils.mkdir(this.yarnConfiguration, outputDir);
        String contents = this.getContent();
        HdfsUtils.writeToFile(this.yarnConfiguration, outputDir + "/diagnostics.json", contents);
        List<String> files = HdfsUtils.getFilesForDir(this.yarnConfiguration, "diagnostics_output");
        Assert.assertEquals(files.size(), 1);

        jobStatus.setDataDiagnosticsPath(files.get(0));

        callable.generateDataDiagnostics(commandState, jobStatus);
        List<ModelCommandLog> logs = this.modelCommandLogService.findByModelCommand(command);
        Assert.assertEquals(logs.size(), 1);
        int warnIndex = logs.get(0).getMessage().contains("The number of skipped rows") ? 0 : 1;
        String warnLog = logs.get(warnIndex).getMessage();
        Assert.assertTrue(warnLog.contains("IsPublicDomain")
                && warnLog.contains("Detected abnormal positive event rate"));
        Assert.assertTrue(warnLog.contains("Uncertainty Coefficient"));
    }

    private String getContent() {
        return " { \"Summary\": { \"SampleSize\": 130768, \"ColumnSize\": 317, \"PositiveEventRate\": 0.0094212651413189772, "
                + "\"NumberOfSkippedRows\": 0, \"NumberOfSkippedRows\": 10, \"HighUCColumns\": \"IsPublicDomain, AwardCategory\"} "
                + " \"MetadataDiagnostics\": { \"IsPublicDomain\": \"DisplayDiscretizationStrategy\" }, "
                + " \"ColumnDiagnostics\": [ { \"Colname\": \"IsPublicDomain\", \"DisplayName\": \"IsPublicDomain\", "
                + " \"PopulationRate\": 1.0, \"Type\": \"Band\",\"BucketingStrategy\": null } ] }";
    }
}
