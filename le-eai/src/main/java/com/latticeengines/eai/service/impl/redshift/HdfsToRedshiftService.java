package com.latticeengines.eai.service.impl.redshift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
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

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Autowired
    private Configuration yarnConfiguration;

    public void uploadDataObjectToS3(HdfsToRedshiftConfiguration configuration) {

        if (!configuration.isAppend()) {
            cleanupS3(configuration);
        }

        HdfsToS3Configuration s3Configuration = new HdfsToS3Configuration();
        s3Configuration.setSplitSize(100L * 1024 * 1024);
        s3Configuration.setS3Bucket(s3Bucket);
        s3Configuration.setS3Prefix(s3Prefix(configuration));
        s3Configuration.setExportInputPath(configuration.getExportInputPath());
        s3Configuration.setTargetFilename(s3FileName(configuration));

        hdfsToS3ExportService.downloadToLocal(s3Configuration);
        hdfsToS3ExportService.upload(s3Configuration);

    }

    public void copyToRedshift(HdfsToRedshiftConfiguration configuration) {
        redshiftService.dropTable(configuration.getTableName());
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, configuration.getExportInputPath());
            RedshiftUtils.generateJsonPathsFile(schema, outputStream);
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                s3Service.uploadInputStream(s3Bucket, configuration.getJsonPathPrefix(), inputStream, true);
            }
            redshiftService.createTable(configuration.getTableName(), schema);
        } catch (IOException e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new RuntimeException(e);
        }
        redshiftService.loadTableFromAvroInS3(configuration.getTableName(), s3Bucket, s3Prefix(configuration),
                configuration.getJsonPathPrefix());

    }

    // should only be used for testing purpose
    public void setS3Bucket(String bucket) {
        s3Bucket = bucket;
    }

    public void cleanupS3(HdfsToRedshiftConfiguration configuration) {
        String prefix = s3Prefix(configuration);
        s3Service.cleanupPrefix(s3Bucket, prefix);
        s3Service.cleanupPrefix(s3Bucket, configuration.getJsonPathPrefix());
    }

    private String s3FileName(HdfsToRedshiftConfiguration configuration) {
        return configuration.getTableName().toLowerCase() + ".avro";
    }

    private String s3Prefix(HdfsToRedshiftConfiguration configuration) {
        return RedshiftUtils.AVRO_STAGE + "/" + configuration.getTableName();
    }

}
