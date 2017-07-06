package com.latticeengines.eai.service.impl.redshift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.eai.service.impl.s3.HdfsToS3ExportService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

@Component("hdfsToRedshiftService")
public class HdfsToRedshiftService {

    private static final Log log = LogFactory.getLog(HdfsToRedshiftService.class);

    @Autowired
    private HdfsToS3ExportService hdfsToS3ExportService;

    @Autowired
    private RedshiftService redshiftService;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private Configuration yarnConfiguration;

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
            redshiftService.replaceTable(stagingTableName, tableName);
        } else {
            redshiftService.loadTableFromAvroInS3(tableName, redshiftTableConfig.getS3Bucket(), s3Prefix(tableName),
                    redshiftTableConfig.getJsonPathPrefix());
        }
    }

    public void updateExistingRows(HdfsToRedshiftConfiguration configuration) {
        RedshiftTableConfiguration redshiftTableConfig = configuration.getRedshiftTableConfiguration();
        String tableName = redshiftTableConfig.getTableName();
        String stagingTableName = tableName + "_staging";
        redshiftService.createStagingTable(stagingTableName, tableName);
        redshiftService.loadTableFromAvroInS3(stagingTableName, redshiftTableConfig.getS3Bucket(), s3Prefix(tableName),
                redshiftTableConfig.getJsonPathPrefix());
        redshiftService.updateExistingRowsFromStagingTable(stagingTableName, tableName,
                redshiftTableConfig.getDistKey()); // we would use dist key as
                                                   // join field
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
