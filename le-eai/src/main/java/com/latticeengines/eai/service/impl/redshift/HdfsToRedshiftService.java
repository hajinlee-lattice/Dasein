package com.latticeengines.eai.service.impl.redshift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.impl.s3.HdfsToS3ExportService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

@Component("hdfsToRedshiftService")
public class HdfsToRedshiftService extends EaiRuntimeService<HdfsToRedshiftConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(HdfsToRedshiftService.class);

    @Inject
    private HdfsToS3ExportService hdfsToS3ExportService;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private S3Service s3Service;

    @Inject
    private Configuration yarnConfiguration;

    @Override
    public void invoke(HdfsToRedshiftConfiguration configuration) {
        if (!configuration.isSkipS3Upload()) {
            cleanupS3(configuration);
            setProgress(0.1f);
            uploadJsonPathSchema(configuration);
            setProgress(0.2f);
            uploadDataObjectToS3(configuration);
            setProgress(0.6f);
        }
        setProgress(0.65f);
        if (configuration.isAppend()) {
            copyToRedshift(configuration);
        } else {
            createRedshiftTableIfNotExist(configuration);
            updateExistingRows(configuration);
        }
        setProgress(0.95f);
    }

    public void uploadDataObjectToS3(HdfsToRedshiftConfiguration configuration) {

        RedshiftTableConfiguration redshiftTableConfig = configuration.getRedshiftTableConfiguration();
        HdfsToS3Configuration s3Configuration = new HdfsToS3Configuration();
        if (configuration.isNoSplit()) {
            s3Configuration.setSplitSize(null);
        } else {
            s3Configuration.setSplitSize(500L * 1024 * 1024);
        }
        s3Configuration.setS3Bucket(redshiftTableConfig.getS3Bucket());
        s3Configuration.setS3Prefix(s3Prefix(redshiftTableConfig.getTableName()));
        s3Configuration.setExportInputPath(configuration.getExportInputPath());
        s3Configuration.setTargetFilename(s3FileName(redshiftTableConfig));

        hdfsToS3ExportService.parallelDownloadToLocal(s3Configuration);
        hdfsToS3ExportService.upload(s3Configuration);
    }

    public void uploadJsonPathSchema(HdfsToRedshiftConfiguration configuration) {
        RedshiftTableConfiguration redshiftTableConfig = configuration.getRedshiftTableConfiguration();
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, configuration.getExportInputPath());
            RedshiftUtils.generateJsonPathsFile(schema, outputStream);
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                s3Service.uploadInputStream(redshiftTableConfig.getS3Bucket(), redshiftTableConfig.getJsonPathPrefix(),
                        inputStream, true);
            }
        } catch (IOException e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public void createRedshiftTableIfNotExist(HdfsToRedshiftConfiguration configuration) {
        RedshiftTableConfiguration redshiftTableConfig = configuration.getRedshiftTableConfiguration();
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, configuration.getExportInputPath());
        redshiftService.createTable(redshiftTableConfig, schema);
    }

    public void dropRedshiftTable(HdfsToRedshiftConfiguration configuration) {
        RedshiftTableConfiguration redshiftTableConfig = configuration.getRedshiftTableConfiguration();
        redshiftService.dropTable(redshiftTableConfig.getTableName());
    }

    public void copyToRedshift(HdfsToRedshiftConfiguration configuration) {
        RedshiftTableConfiguration redshiftTableConfig = configuration.getRedshiftTableConfiguration();
        String tableName = redshiftTableConfig.getTableName();
        if (configuration.isCreateNew()) {
            String stagingTableName = tableName + "_staging";
            redshiftTableConfig.setTableName(stagingTableName);
            dropRedshiftTable(configuration);
            createRedshiftTableIfNotExist(configuration);
            redshiftService.loadTableFromAvroInS3(stagingTableName, redshiftTableConfig.getS3Bucket(),
                    s3Prefix(tableName), redshiftTableConfig.getJsonPathPrefix());
            redshiftService.analyzeTable(stagingTableName);
            redshiftService.replaceTable(stagingTableName, tableName);
            Long countAfter = redshiftService.countTable(tableName);
            log.info("Created a table with " + countAfter + " rows: " + tableName);
        } else {
            redshiftService.loadTableFromAvroInS3(tableName, redshiftTableConfig.getS3Bucket(), s3Prefix(tableName),
                    redshiftTableConfig.getJsonPathPrefix());
            // redshiftService.vacuumTable(tableName);
            redshiftService.analyzeTable(tableName);
        }
    }

    public void updateExistingRows(HdfsToRedshiftConfiguration configuration) {
        RedshiftTableConfiguration redshiftTableConfig = configuration.getRedshiftTableConfiguration();
        String tableName = redshiftTableConfig.getTableName();
        String stagingTableName = tableName + "_staging";
        redshiftService.createStagingTable(stagingTableName, tableName);
        redshiftService.loadTableFromAvroInS3(stagingTableName, redshiftTableConfig.getS3Bucket(), s3Prefix(tableName),
                redshiftTableConfig.getJsonPathPrefix());
        Long countStage = redshiftService.countTable(stagingTableName);
        log.info("After staging, there are " + countStage + " rows in " + stagingTableName);
        Long countBefore = redshiftService.countTable(tableName);
        log.info("Before update, there are " + countBefore + " rows in " + tableName);
        redshiftService.updateExistingRowsFromStagingTable(stagingTableName, tableName,
                redshiftTableConfig.getDistKey()); // we would use dist key as
                                                   // join field
        // redshiftService.vacuumTable(tableName);
        redshiftService.analyzeTable(tableName);
        Long countAfter = redshiftService.countTable(tableName);
        log.info("After update, there are " + countAfter + " rows in " + tableName);
        redshiftService.dropTable(stagingTableName);
    }

    public void cleanupS3(HdfsToRedshiftConfiguration configuration) {
        if (!configuration.isCleanupS3()) {
            return;
        }
        RedshiftTableConfiguration redshiftTableConfig = configuration.getRedshiftTableConfiguration();
        String prefix = s3Prefix(redshiftTableConfig.getTableName());
        s3Service.cleanupPrefix(redshiftTableConfig.getS3Bucket(), prefix);
        s3Service.cleanupPrefix(redshiftTableConfig.getS3Bucket(), redshiftTableConfig.getJsonPathPrefix());
    }

    private String s3FileName(RedshiftTableConfiguration configuration) {
        return configuration.getTableName().toLowerCase() + ".avro";
    }

    private String s3Prefix(String tableName) {
        return RedshiftUtils.AVRO_STAGE + "/" + tableName;
    }

}
