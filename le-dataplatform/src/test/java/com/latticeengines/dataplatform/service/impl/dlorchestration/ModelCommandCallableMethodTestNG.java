package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.impl.AlertServiceImpl;
import com.latticeengines.dataplatform.exposed.service.impl.PagerDutyTestUtils;
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
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    @Autowired
    private DebugProcessorImpl debugProcessorImpl;

    @Autowired
    private AlertServiceImpl alertService;

    @Value("${dataplatform.fs.web.defaultFS}")
    private String httpFsPrefix;

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
    
    @BeforeClass(groups = "functional")
    public void setup() {
        alertService.enableTestMode();
    }

    @Test(groups = "functional")
    public void testHandleJobFailed() throws ParseException {
        ModelCommand command = new ModelCommand(1L, "Nutanix", ModelCommandStatus.NEW, null, ModelCommand.TAHOE);
        command.setModelCommandStep(ModelCommandStep.PROFILE_DATA);
        modelCommandEntityMgr.create(command);

        modelCommandLogService.log(command, "message.  #%#$%%^$%^$%^$%^");
        modelCommandLogService.log(command, "another message.  #%#$%%^$%^$%^$%^ 12344       .");

        ModelCommandResult result = new ModelCommandResult(command, new Date(), new Date(),
                ModelCommandStatus.IN_PROGRESS);
        modelCommandResultEntityMgr.create(result);

        ModelCommandCallable callable = new ModelCommandCallable(command, yarnConfiguration, modelingJobService,
                modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepYarnProcessor, modelCommandLogService,
                modelCommandResultEntityMgr, modelStepFinishProcessor, modelStepOutputResultsProcessor,
                modelStepRetrieveMetadataProcessor, debugProcessorImpl, alertService, httpFsPrefix,
                resourceManagerWebAppAddress, appTimeLineWebAppAddress, rowFailThreshold, rowWarnThreshold, positiveEventFailThreshold, positiveEventWarnThreshold);

        PagerDutyTestUtils.confirmPagerDutyIncident(callable.handleJobFailed());

        List<String> failedAppIds = new ArrayList<String>();
        failedAppIds.add("application_1415144508340_0729");
        PagerDutyTestUtils.confirmPagerDutyIncident(callable.handleJobFailed(failedAppIds));
    }

    @Test(groups = "functional")
    public void generateDataDiagnostics() throws Exception {
        ModelCommand command = new ModelCommand(2L, "Nutanix", ModelCommandStatus.NEW, null, ModelCommand.TAHOE);
        command.setModelCommandStep(ModelCommandStep.SUBMIT_MODELS);
        modelCommandEntityMgr.create(command);

        ModelCommandCallable callable = new ModelCommandCallable(command, yarnConfiguration, modelingJobService,
                modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepYarnProcessor, modelCommandLogService,
                modelCommandResultEntityMgr, modelStepFinishProcessor, modelStepOutputResultsProcessor,
                modelStepRetrieveMetadataProcessor, debugProcessorImpl, alertService, httpFsPrefix,
                resourceManagerWebAppAddress, appTimeLineWebAppAddress, rowFailThreshold, rowWarnThreshold, positiveEventFailThreshold, positiveEventWarnThreshold);
        ModelCommandState commandState = new ModelCommandState(command, ModelCommandStep.SUBMIT_MODELS);
        JobStatus jobStatus = new JobStatus();

        String outputDir = "diagnostics_output";
        HdfsUtils.rmdir(yarnConfiguration, outputDir);
        HdfsUtils.mkdir(yarnConfiguration, outputDir);
        String contents = getContent();
        HdfsUtils.writeToFile(yarnConfiguration, outputDir + "/diagnostics.json", contents);
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, "diagnostics_output");
        Assert.assertEquals(files.size(), 1);

        jobStatus.setDataDiagnosticsPath(files.get(0));

        callable.generateDataDiagnostics(commandState, jobStatus);
        List<ModelCommandLog> logs = modelCommandLogService.findByModelCommand(command);
        Assert.assertEquals(logs.size(), 2);
        int warnIndex = logs.get(0).getMessage().contains("The number of skipped rows") ? 0 : 1;
        String warnLog = logs.get(warnIndex).getMessage();
        Assert.assertTrue(warnLog.contains("IsPublicDomain")
                && warnLog.contains("Detected abnormal positive event rate"));

        int linkIndex = warnIndex == 0 ? 1 : 0;
        String linkLog = logs.get(linkIndex).getMessage();
        Assert.assertTrue(linkLog.contains("http"));
        HdfsUtils.rmdir(yarnConfiguration, outputDir);

    }

    private String getContent() {
        return " { \"Summary\": { \"SampleSize\": 130768, \"ColumnSize\": 317, \"PositiveEventRate\": 0.0094212651413189772, \"NumberOfSkippedRows\": 0, \"NumberOfSkippedRows\": 10} "
                + " \"MetadataDiagnostics\": { \"IsPublicDomain\": \"DisplayDiscretizationStrategy\" }, "
                + " \"ColumnDiagnostics\": [ { \"Colname\": \"IsPublicDomain\", \"DisplayName\": \"IsPublicDomain\", "
                + " \"PopulationRate\": 1.0, \"Type\": \"Band\",\"BucketingStrategy\": null } ] }";
    }
}
