package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.ingestion.SqlToSourceConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SqlToTextConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("ingestionSQLProviderService")
public class IngestionSQLProviderServiceImpl extends IngestionProviderServiceImpl {
    private static final Logger log = LoggerFactory.getLogger(IngestionSQLProviderServiceImpl.class);

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private IngestionVersionService ingestionVersionService;

    @Autowired
    private SqoopProxy sqoopProxy;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private SourceService sourceService;

    @Autowired
    protected JobService jobService;

    private static final Integer WORKFLOW_WAIT_TIME_IN_SECOND = (int) TimeUnit.HOURS.toSeconds(24);
    private static final String sqoopPrefix = "part-m-";
    private static final String SQOOP_OPTION_WHERE = "--where";

    @Override
    public void ingest(IngestionProgress progress) throws Exception {
        switch (progress.getIngestion().getIngestionType()) {
        case SQL_TO_CSVGZ:
            ingestBySqoopToCSVGZ(progress);
            break;
        case SQL_TO_SOURCE:
            ingestBySqoopToSource(progress);
            break;
        default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion type %s is not supported", progress.getIngestion().getIngestionType()));
        }
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        return null;
    }

    private void ingestBySqoopToSource(IngestionProgress progress) throws Exception {
        SqlToSourceConfiguration config = (SqlToSourceConfiguration) progress.getIngestion().getProviderConfiguration();
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.host(config.getDbHost()).port(config.getDbPort()).db(config.getDb()).user(config.getDbUser())
                .encryptedPassword(config.getDbPwdEncrypted());
        Path hdfsDir = new Path(progress.getDestination());
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir.toString())) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsDir.toString());
        }

        SqoopImporter.Builder builder = new SqoopImporter.Builder().setCustomer(progress.getTriggeredBy())
                .setNumMappers(config.getMappers()).setSplitColumn(config.getTimestampColumn())
                .setTable(config.getDbTable()).setTargetDir(progress.getDestination())
                .setDbCreds(new DbCreds(credsBuilder)).setSync(false)
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        StringBuilder whereClause = new StringBuilder();
        switch (config.getCollectCriteria()) {
        case NEW_DATA:
            whereClause.append("\"");
            String currentVersion = hdfsSourceEntityMgr.getCurrentVersion(config.getSource());
            Date startDate = null;
            if (currentVersion != null) {
                startDate = HdfsPathBuilder.dateFormat.parse(currentVersion);
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                whereClause.append(
                        String.format("%s > '%s' AND ", config.getTimestampColumn(), dateFormat.format(startDate)));
            }
            Date endDate = HdfsPathBuilder.dateFormat.parse(progress.getVersion());
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            whereClause.append(String.format("%s <= '%s'", config.getTimestampColumn(), dateFormat.format(endDate)));
            whereClause.append("\"");
            log.info(String.format("Selected date range (%s, %s]", startDate, endDate));
            break;
        default:
            break;
        }
        if (whereClause.length() > 0) {
            builder = builder.addExtraOption(SQOOP_OPTION_WHERE).addExtraOption(whereClause.toString());
        }
        SqoopImporter importer = builder.build();

        ApplicationId appId = ConverterUtils
                .toApplicationId(sqoopProxy.importData(importer).getApplicationIds().get(0));
        JobStatus finalStatus = jobService.waitFinalJobStatus(appId.toString(), WORKFLOW_WAIT_TIME_IN_SECOND);
        if (finalStatus.getStatus() == FinalApplicationStatus.SUCCEEDED
                && ingestionVersionService.isCompleteVersion(progress.getIngestion(), progress.getVersion())) {
            Source source = sourceService.findBySourceName(config.getSource());
            log.info(String.format("Counting total records in source %s for version %s...", source.getSourceName(),
                    progress.getVersion()));
            Long count = hdfsSourceEntityMgr.count(source, progress.getVersion());
            log.info(String.format("Total records in source %s for version %s: %d", source.getSourceName(),
                    progress.getVersion(), count));
            ingestionVersionService.updateCurrentVersion(progress.getIngestion(), progress.getVersion());
            progress = ingestionProgressService.updateProgress(progress).size(count).status(ProgressStatus.FINISHED)
                    .commit(true);
            log.info("Ingestion finished. Progress: " + progress.toString());
        } else {
            progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                    .errorMessage("Sqoop upload failed: " + importer.toString()).commit(true);
            log.error("Ingestion failed. Progress: " + progress.toString() + " SqoopImporter: " + importer.toString());
        }

    }

    private void ingestBySqoopToCSVGZ(IngestionProgress progress) throws Exception {
        SqlToTextConfiguration config = (SqlToTextConfiguration) progress.getIngestion().getProviderConfiguration();
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.host(config.getDbHost()).port(config.getDbPort()).db(config.getDb()).user(config.getDbUser())
                .encryptedPassword(config.getDbPwdEncrypted());
        Path hdfsDir = new Path(progress.getDestination()).getParent();
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir.toString())) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsDir.toString());
        }
        SqoopImporter importer = new SqoopImporter.Builder() //
                .setCustomer(progress.getTriggeredBy()) //
                .setTable(config.getDbTable()) //
                .setTargetDir(hdfsDir.toString()) //
                .setDbCreds(new DbCreds(credsBuilder)) //
                .setMode(config.getSqoopMode()) //
                .setQuery(config.getDbQuery()) //
                .setNumMappers(config.getMappers()) //
                .setSplitColumn(config.getSplitColumn()) //
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission()) //
                .build();
        List<String> otherOptions = new ArrayList<>(Arrays.asList("--relaxed-isolation", "--as-textfile"));
        if (!StringUtils.isEmpty(config.getNullString())) {
            otherOptions.add("--null-string");
            otherOptions.add(config.getNullString());
            otherOptions.add("--null-non-string");
            otherOptions.add(config.getNullString());
        }
        if (!StringUtils.isEmpty(config.getEnclosedBy())) {
            otherOptions.add("--enclosed-by");
            otherOptions.add(config.getEnclosedBy());
        }
        if (!StringUtils.isEmpty(config.getOptionalEnclosedBy())) {
            otherOptions.add("--optionally-enclosed-by");
            otherOptions.add(config.getOptionalEnclosedBy());
        }
        if (!StringUtils.isEmpty(config.getEscapedBy())) {
            otherOptions.add("--escaped-by");
            otherOptions.add(config.getEscapedBy());
        }
        if (!StringUtils.isEmpty(config.getFieldTerminatedBy())) {
            otherOptions.add("--fields-terminated-by");
            otherOptions.add(config.getFieldTerminatedBy());
        }
        importer.setOtherOptions(otherOptions);
        ApplicationId appId = ConverterUtils
                .toApplicationId(sqoopProxy.importData(importer).getApplicationIds().get(0));
        JobStatus finalStatus = jobService.waitFinalJobStatus(appId.toString(), WORKFLOW_WAIT_TIME_IN_SECOND);
        if (finalStatus.getStatus() != FinalApplicationStatus.SUCCEEDED) {
            throw new RuntimeException(
                    "Application final status is not " + FinalApplicationStatus.SUCCEEDED.toString());
        }
        for (int i = 0; i < config.getMappers(); i++) {
            String sqoopPostfix = String.format("%05d", i);
            Path file = new Path(hdfsDir, sqoopPrefix + sqoopPostfix);
            if (!waitForFileToBeIngested(file.toString())) {
                throw new RuntimeException("Failed to download " + file.toString());
            }
        }
        renameFiles(progress);
        if (config.isCompressed()) {
            compressGzipFiles(progress);
        }
        progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FINISHED).commit(true);
    }

    private void renameFiles(IngestionProgress progress) throws Exception {
        SqlToTextConfiguration sqlToTextConfig = (SqlToTextConfiguration) progress.getIngestion()
                .getProviderConfiguration();
        Path hdfsDir = new Path(progress.getDestination()).getParent();
        for (int i = 0; i < sqlToTextConfig.getMappers(); i++) {
            String sqoopPostfix = String.format("%05d", i);
            Path originFile = new Path(hdfsDir, sqoopPrefix + sqoopPostfix);
            Path renameFile = new Path(progress.getDestination());
            renameFile = new Path(hdfsDir,
                    renameFile.getName() + (sqlToTextConfig.getMappers() > 1 ? "_" + sqoopPostfix : "")
                            + (sqlToTextConfig.getFileExtension() == null ? "" : sqlToTextConfig.getFileExtension()));
            try (FileSystem fs = FileSystem.newInstance(yarnConfiguration)) {
                fs.rename(originFile, renameFile);
            }
        }
    }

    private void compressGzipFiles(IngestionProgress progress) throws Exception {
        SqlToTextConfiguration sqlToTextConfig = (SqlToTextConfiguration) progress.getIngestion()
                .getProviderConfiguration();
        Path hdfsDir = new Path(progress.getDestination()).getParent();
        for (int i = 0; i < sqlToTextConfig.getMappers(); i++) {
            Path destFile = new Path(progress.getDestination());
            Path gzHdfsPath = new Path(hdfsDir,
                    destFile.getName() + (sqlToTextConfig.getMappers() > 1 ? "_" + String.format("%05d", i) : "")
                            + (sqlToTextConfig.getFileExtension() == null ? "" : sqlToTextConfig.getFileExtension())
                            + EngineConstants.GZ);
            Path uncompressedFilePath = new Path(hdfsDir,
                    destFile.getName() + (sqlToTextConfig.getMappers() > 1 ? "_" + String.format("%05d", i) : "")
                            + (sqlToTextConfig.getFileExtension() == null ? "" : sqlToTextConfig.getFileExtension()));
            HdfsUtils.compressGZFileWithinHDFS(yarnConfiguration, gzHdfsPath.toString(),
                    uncompressedFilePath.toString());
            HdfsUtils.rmdir(yarnConfiguration, uncompressedFilePath.toString());
        }
    }
}
