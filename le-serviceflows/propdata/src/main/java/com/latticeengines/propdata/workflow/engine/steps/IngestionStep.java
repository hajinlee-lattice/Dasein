package com.latticeengines.propdata.workflow.engine.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.hsqldb.lib.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SqlToTextConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("ingestionStep")
@Scope("prototype")
public class IngestionStep extends BaseWorkflowStep<IngestionStepConfiguration> {
    private static final Log log = LogFactory.getLog(IngestionStep.class);

    private IngestionProgress progress;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private SqoopProxy sqoopProxy;

    private YarnClient yarnClient;

    private static final long WORKFLOW_WAIT_TIME_IN_MILLIS = TimeUnit.HOURS.toMillis(6);
    private static final long MAX_MILLIS_TO_WAIT = TimeUnit.HOURS.toMillis(5);
    private static final Integer WORKFLOW_WAIT_TIME_IN_SECOND = (int) TimeUnit.HOURS.toSeconds(6);

    private static final String sqoopPrefix = "part-m-";

    @Override
    public void execute() {
        try {
            log.info("Entering IngestionStep execute()");
            progress = getConfiguration().getIngestionProgress();
            HdfsPodContext.changeHdfsPodId(progress.getHdfsPod());
            Ingestion ingestion = getConfiguration().getIngestion();
            ProviderConfiguration providerConfiguration = getConfiguration()
                    .getProviderConfiguration();
            ingestion.setProviderConfiguration(providerConfiguration);
            progress.setIngestion(ingestion);
            initializeYarnClient();
            switch (progress.getIngestion().getIngestionType()) {
                case SFTP:
                    ingestionFromSftp();
                    break;
                case SQL_TO_CSVGZ:
                    ingestionBySqoop();
                    break;
            default:
                break;
            }

            log.info("Exiting IngestionStep execute()");
        } catch (Exception e) {
            failByException(e);
        } finally {
            try {
                yarnClient.close();
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    private void ingestionFromSftp() throws Exception {
        String destFile = progress.getDestination();
        Path tmpDestDir = new Path(new Path(progress.getDestination()).getParent(),
                "TMP_" + UUID.randomUUID().toString());
        Path tmpDestFile = new Path(tmpDestDir, new Path(progress.getDestination()).getName());
        progress.setDestination(tmpDestFile.toString());
        CamelRouteConfiguration camelRouteConfig = ingestionProgressService
                .createCamelRouteConfiguration(progress);
        ImportConfiguration importConfig = ImportConfiguration
                .createForCamelRouteConfiguration(camelRouteConfig);
        progress.setDestination(destFile);
        AppSubmission submission = eaiProxy.createImportDataJob(importConfig);
        String eaiAppId = submission.getApplicationIds().get(0);
        log.info("EAI Service ApplicationId: " + eaiAppId);
        FinalApplicationStatus status = waitForStatus(eaiAppId, WORKFLOW_WAIT_TIME_IN_MILLIS,
                FinalApplicationStatus.SUCCEEDED);

        if (status == FinalApplicationStatus.SUCCEEDED
                && waitForFileToBeDownloaded(tmpDestFile.toString())) {
            HdfsUtils.moveFile(yarnConfiguration, tmpDestFile.toString(),
                    progress.getDestination());
            Long size = HdfsUtils.getFileSize(yarnConfiguration, progress.getDestination());
            progress = ingestionProgressService.updateProgress(progress).size(size)
                    .status(ProgressStatus.FINISHED).commit(true);
            HdfsUtils.rmdir(yarnConfiguration, tmpDestDir.toString());
            log.info("Ingestion finished. Progress: " + progress.toString());
        } else {
            progress = ingestionProgressService.updateProgress(progress)
                    .status(ProgressStatus.FAILED).commit(true);
            log.error("Ingestion failed. Progress: " + progress.toString());
        }
    }

    private void ingestionBySqoop() throws Exception {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        SqlToTextConfiguration sqlToTextConfig = (SqlToTextConfiguration) progress.getIngestion()
                .getProviderConfiguration();
        credsBuilder.jdbcUrl(sqlToTextConfig.getDbUrl()).driverClass(sqlToTextConfig.getDbDriver())
                .user(sqlToTextConfig.getDbUserName())
                .encryptedPassword(sqlToTextConfig.getDbPasswordEncrypted());
        Path hdfsDir = new Path(progress.getDestination()).getParent();
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir.toString())) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsDir.toString());
        }
        SqoopImporter importer = new SqoopImporter.Builder() //
                .setCustomer(progress.getTriggeredBy()) //
                .setTable(sqlToTextConfig.getDbTable()) //
                .setTargetDir(hdfsDir.toString()) //
                .setDbCreds(new DbCreds(credsBuilder)) //
                .setMode(sqlToTextConfig.getSqoopMode()) //
                .setQuery(sqlToTextConfig.getDbQuery()) //
                .setNumMappers(sqlToTextConfig.getMappers()) //
                .setSplitColumn(sqlToTextConfig.getSplitColumn()) //
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission()) //
                .build();
        List<String> otherOptions = new ArrayList<>(Arrays.asList("--relaxed-isolation", "--as-textfile"));
        if (!StringUtil.isEmpty(sqlToTextConfig.getNullString())) {
            otherOptions.add("--null-string");
            otherOptions.add(sqlToTextConfig.getNullString());
            otherOptions.add("--null-non-string");
            otherOptions.add(sqlToTextConfig.getNullString());
        }
        if (!StringUtil.isEmpty(sqlToTextConfig.getEnclosedBy())) {
            otherOptions.add("--enclosed-by");
            otherOptions.add(sqlToTextConfig.getEnclosedBy());
        }
        if (!StringUtil.isEmpty(sqlToTextConfig.getOptionalEnclosedBy())) {
            otherOptions.add("--optionally-enclosed-by");
            otherOptions.add(sqlToTextConfig.getOptionalEnclosedBy());
        }
        if (!StringUtil.isEmpty(sqlToTextConfig.getEscapedBy())) {
            otherOptions.add("--escaped-by");
            otherOptions.add(sqlToTextConfig.getEscapedBy());
        }
        if (!StringUtil.isEmpty(sqlToTextConfig.getFieldTerminatedBy())) {
            otherOptions.add("--fields-terminated-by");
            otherOptions.add(sqlToTextConfig.getFieldTerminatedBy());
        }
        importer.setOtherOptions(otherOptions);
        ApplicationId appId = ConverterUtils.toApplicationId(sqoopProxy.importData(importer).getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, WORKFLOW_WAIT_TIME_IN_SECOND);
        if (finalStatus != FinalApplicationStatus.SUCCEEDED) {
            throw new RuntimeException("Application final status is not " + FinalApplicationStatus.SUCCEEDED.toString());
        }
        for (int i = 0; i < sqlToTextConfig.getMappers(); i++) {
            String sqoopPostfix = String.format("%05d", i);
            Path file = new Path(hdfsDir, sqoopPrefix + sqoopPostfix);
            if (!waitForFileToBeDownloaded(file.toString())) {
                throw new RuntimeException("Failed to download " + file.toString());
            }
        }
        renameFiles();
        if (sqlToTextConfig.isCompressed()) {
            compressGzipFiles();
        }
        progress = ingestionProgressService.updateProgress(progress)
                .status(ProgressStatus.FINISHED).commit(true);
    }

    private void renameFiles() throws Exception {
        SqlToTextConfiguration sqlToTextConfig = (SqlToTextConfiguration) progress.getIngestion()
                .getProviderConfiguration();
        Path hdfsDir = new Path(progress.getDestination()).getParent();
        for (int i = 0; i < sqlToTextConfig.getMappers(); i++) {
            String sqoopPostfix = String.format("%05d", i);
            Path originFile = new Path(hdfsDir, sqoopPrefix + sqoopPostfix);
            Path renameFile = new Path(progress.getDestination());
            renameFile = new Path(hdfsDir,
                    renameFile.getName()
                            + (sqlToTextConfig.getMappers() > 1 ? "_" + sqoopPostfix : "")
                            + (sqlToTextConfig.getFileExtension() == null ? ""
                                    : sqlToTextConfig.getFileExtension()));
            try (FileSystem fs = FileSystem.newInstance(yarnConfiguration)) {
                fs.rename(originFile, renameFile);
            }
        }
    }

    private void compressGzipFiles() throws Exception {
        SqlToTextConfiguration sqlToTextConfig = (SqlToTextConfiguration) progress.getIngestion()
                .getProviderConfiguration();
        Path hdfsDir = new Path(progress.getDestination()).getParent();
        for (int i = 0; i < sqlToTextConfig.getMappers(); i++) {
            Path destFile = new Path(progress.getDestination());
            Path gzHdfsPath = new Path(hdfsDir,
                    destFile.getName()
                            + (sqlToTextConfig.getMappers() > 1 ? "_" + String.format("%05d", i)
                                    : "")
                            + (sqlToTextConfig.getFileExtension() == null ? ""
                                    : sqlToTextConfig.getFileExtension())
                            + EngineConstants.GZ);
            Path uncompressedFilePath = new Path(hdfsDir,
                    destFile.getName()
                            + (sqlToTextConfig.getMappers() > 1 ? "_" + String.format("%05d", i)
                                    : "")
                            + (sqlToTextConfig.getFileExtension() == null ? ""
                                    : sqlToTextConfig.getFileExtension()));
            HdfsUtils.compressGZFileWithinHDFS(yarnConfiguration, gzHdfsPath.toString(),
                    uncompressedFilePath.toString());
            HdfsUtils.rmdir(yarnConfiguration, uncompressedFilePath.toString());
        }
    }

    private void failByException(Exception e) {
        progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                .errorMessage(e.getMessage()).commit(true);
        log.error("Ingestion failed: " + e.getMessage() + ". Progress: " + progress.toString());
    }

    private void initializeYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
    }

    private FinalApplicationStatus waitForStatus(String applicationId, Long waitTimeInMillis,
            FinalApplicationStatus... applicationStatuses) throws Exception {
        waitTimeInMillis = waitTimeInMillis == null ? MAX_MILLIS_TO_WAIT : waitTimeInMillis;
        log.info(String.format("Waiting on %s for at most %dms.", applicationId, waitTimeInMillis));

        FinalApplicationStatus status = null;
        long start = System.currentTimeMillis();

        done: do {
            ApplicationReport report = yarnClient
                    .getApplicationReport(ConverterUtils.toApplicationId(applicationId));
            status = report.getFinalApplicationStatus();
            if (status == null) {
                break;
            }
            for (FinalApplicationStatus statusCheck : applicationStatuses) {
                if (status.equals(statusCheck) || YarnUtils.TERMINAL_STATUS.contains(status)) {
                    break done;
                }
            }
            Thread.sleep(1000);
        } while (System.currentTimeMillis() - start < waitTimeInMillis);
        return status;
    }

    private boolean waitForFileToBeDownloaded(String destPath) {
        Long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 10000) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, destPath)) {
                    return true;
                }
                Thread.sleep(1000L);
            } catch (Exception e) {
                // ignore
            }
        }
        return false;
    }
}