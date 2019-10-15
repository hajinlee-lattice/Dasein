package com.latticeengines.apps.cdl.testframework;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

public abstract class CDLWorkflowFrameworkDeploymentTestNGBase extends CDLWorkflowFrameworkTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLWorkflowFrameworkDeploymentTestNGBase.class);

    private static final String DEFAULT_SYSTEM = "DefaultSystem";

    private static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";
    private static final String S3_AVRO_DIR = "le-serviceapps/cdl/end2end/avro";
    private static final String S3_AVRO_VERSION = "6";
    private static final long MAX_WORKFLOW_RUNTIME_IN_HOURS = 2;

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected TestArtifactService testArtifactService;

    /*
     * CDL versions
     */
    protected DataCollection.Version active = DataCollection.Version.Blue;
    protected DataCollection.Version inactive = active.complement();

    protected void setupTestEnvironment() {
        try {
            setupEnd2EndTestEnvironment(null);
            mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
            setupYarnPlatform();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void setupYarnPlatform() {
        platformTestBase = new YarnFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

    protected void setupEnd2EndTestEnvironment(Map<String, Boolean> featureFlagMap) {
        setupTestEnvironmentWithFeatureFlags(featureFlagMap);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
    }

    /*-
     * run workflow in hadoop instead of in memory, need this if workflow use transformer
     */
    protected void runWorkflowRemote(WorkflowConfiguration workflowConfig) throws Exception {
        AppSubmission submission = workflowProxy.submitWorkflowExecution(workflowConfig, mainCustomerSpace);
        String applicationId = submission.getApplicationIds().get(0);

        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId, mainCustomerSpace);
        Long pid = job.getPid();
        JobStatus status = job.getJobStatus();

        Instant start = Instant.now();
        log.info("Waiting for job (appId={},PID={}) to finish. Starting at {}", applicationId, pid, start);
        Instant endThreshold = start.plus(MAX_WORKFLOW_RUNTIME_IN_HOURS, ChronoUnit.HOURS);
        while (status == null || !status.isTerminated()) {
            if (Instant.now().isAfter(endThreshold)) {
                throw new IllegalStateException(String.format("Job (appId=%s,PID=%d) does not finish within %d hours",
                        applicationId, pid, MAX_WORKFLOW_RUNTIME_IN_HOURS));
            }
            Thread.sleep(20000L);
            job = workflowProxy.getJobByWorkflowJobPid(mainCustomerSpace, pid);
            if (job != null) {
                // update job status & applicationId (in case of workflow being queued)
                status = job.getJobStatus();
                applicationId = job.getApplicationId();
            }
        }
        log.info("Job (appId={},PID={}) finishes with duration={}, status={}", applicationId, pid,
                Duration.between(start, Instant.now()), status);
        Assert.assertEquals(status, JobStatus.COMPLETED);
    }

    /*
     * Helper to fake context in workflow
     */

    protected void setCDLVersions(@NotNull WorkflowConfiguration config,
            @NotNull DataCollection.Version activeVersion) {
        addInitialContextIfNotExists(config);
        config.getInitialContext().put(BaseWorkflowStep.CDL_ACTIVE_VERSION, JsonUtils.serialize(activeVersion));
        config.getInitialContext().put(BaseWorkflowStep.CDL_INACTIVE_VERSION,
                JsonUtils.serialize(activeVersion.complement()));
        log.info("Set active version to {} and inactive version to {}", activeVersion, activeVersion.complement());
    }

    protected long countRecordsInTable(@NotNull String tableName) {
        String tablePath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), mainTestCustomerSpace)
                .append(tableName).toString();
        log.info("Counting records in table {}, path={}", tableName, tablePath);
        return AvroUtils.count(yarnConfiguration, tablePath + "/*.avro");
    }

    protected void swapCDLVersions() {
        active = active.complement();
        inactive = inactive.complement();
    }

    protected void skipPublishToS3(@NotNull WorkflowConfiguration config) {
        addInitialContextIfNotExists(config);
        config.getInitialContext().put(BaseWorkflowStep.SKIP_PUBLISH_PA_TO_S3, Boolean.TRUE.toString());
    }

    private void addInitialContextIfNotExists(@NotNull WorkflowConfiguration config) {
        if (config.getInitialContext() == null) {
            config.setInitialContext(new HashMap<>());
        }
    }

    protected String getFeedTypeByEntity(@NotNull EntityType entity) {
        return DEFAULT_SYSTEM + "_" + entity.getDefaultFeedTypeName();
    }

    protected void clearAllTables(DataCollection.Version version) {
        dataCollectionProxy.unlinkTables(mainCustomerSpace, version);
    }

    /*-
     * Import csv file from test artifact dir with given import template
     * file format = <entity>_<suffix>-<fileIdx>.avro
     */
    protected List<String> mockCSVImport(BusinessEntity entity, String suffix, int fileIdx, DataFeedTask dataFeedTask) {
        String templateName = dataFeedTask.getImportTemplate().getName();
        Pair<String, InputStream> testAvroArtifact = getTestAvroFile(entity, suffix, fileIdx);
        String fileName = testAvroArtifact.getLeft();
        CustomerSpace customerSpace = CustomerSpace.parse(mainCustomerSpace);
        String tablePath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
        String extractDir = new SimpleDateFormat(COLLECTION_DATE_FORMAT).format(new Date()) + "_"
                + RandomStringUtils.randomAlphanumeric(6);
        String extractPath = String.format("%s/%s/Extracts/%s", tablePath, SourceType.FILE.getName(),
                extractDir);
        long numRecords;
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, testAvroArtifact.getRight(),
                    extractPath + "/part-00000.avro");
            numRecords = AvroUtils.count(yarnConfiguration, extractPath + "/*.avro");
            log.info("Uploaded {} records from {} to {}", numRecords, fileName, extractPath);
        } catch (IOException e) {
            log.error("Failed to upload file {}, error={}", fileName, e);
            throw new RuntimeException(e);
        }
        Extract extract = new Extract();
        extract.setName("Extract-" + templateName);
        extract.setPath(extractPath);
        extract.setProcessedRecords(numRecords);
        extract.setExtractionTimestamp(System.currentTimeMillis());
        return dataFeedProxy.registerExtract(customerSpace.toString(), dataFeedTask.getUniqueId(), templateName,
                extract);
    }

    /*-
     * Upsert DataFeedTask (named destFeedType) using a serialized import template in test artifact dir
     * import template file format = <entity>_<suffix>_<srcFeedType>.json
     */
    protected DataFeedTask registerMockDataFeedTask(BusinessEntity entity, String suffix, String srcFeedType,
            String destFeedType, DataFeedTask.IngestionBehavior behavior) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), SourceType.FILE.getName(),
                destFeedType, entity.name());
        if (dataFeedTask == null) {
            dataFeedTask = new DataFeedTask();
            Table importTemplate = getMockTemplate(entity, suffix, srcFeedType);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            importTemplate.setName(NamingUtils.timestamp(entity.name()));
            dataFeedTask.setImportTemplate(importTemplate);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity.name());
            dataFeedTask.setFeedType(destFeedType);
            dataFeedTask.setSource(SourceType.FILE.getName());
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedTask.setIngestionBehavior(behavior);
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
        }
        return dataFeedTask;
    }

    private Table getMockTemplate(BusinessEntity entity, String suffix, String feedType) {
        String templateFileName = String.format("%s_%s_%s.json", entity.name(), suffix, feedType);
        InputStream templateIs = testArtifactService.readTestArtifactAsStream(S3_AVRO_DIR, S3_AVRO_VERSION,
                templateFileName);
        try {
            ObjectMapper om = new ObjectMapper();
            return om.readValue(templateIs, Table.class);
        } catch (IOException e) {
            log.error("Failed to read import template from file {}, error={}", templateFileName, e);
            throw new RuntimeException(e);
        }
    }

    private Pair<String, InputStream> getTestAvroFile(BusinessEntity entity, String suffix, int fileIdx) {
        String fileName = String.format("%s_%s-%d.avro", entity.name(), suffix, fileIdx);
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_AVRO_DIR, S3_AVRO_VERSION, fileName);
        return Pair.of(fileName, is);
    }

}
