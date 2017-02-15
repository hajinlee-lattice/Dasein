package com.latticeengines.eai.service.impl.redshift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

@Component("hdfsToRedshiftService")
public class HdfsToRedshiftService {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(HdfsToRedshiftService.class);

    @Autowired
    private RedshiftService redshiftService;

    @Autowired
    private S3Service s3Service;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Autowired
    private Configuration yarnConfiguration;

    public void uploadToS3(HdfsToRedshiftConfiguration configuration) throws IOException {
        try (InputStream avroStream = HdfsUtils.getInputStream(yarnConfiguration, configuration.getExportInputPath())) {
            s3Service.uploadInputStream(s3Bucket, s3Prefix(configuration), avroStream, true);
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, configuration.getExportInputPath());
            RedshiftUtils.generateJsonPathsFile(schema, outputStream);
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                s3Service.uploadInputStream(s3Bucket, configuration.getJsonPathPrefix(), inputStream, true);
            }
        }

    }

    public void copyToRedshift(HdfsToRedshiftConfiguration configuration) {
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, configuration.getExportInputPath());
        redshiftService.createTable(configuration.getTableName(), schema);
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
    }

//    private String s3FileName(HdfsToRedshiftConfiguration configuration) {
//        return configuration.getTableName().toLowerCase() + ".avro";
//    }

    private String s3Prefix(HdfsToRedshiftConfiguration configuration) {
        return RedshiftUtils.AVRO_STAGE + "/" + configuration.getTableName();
    }

}
