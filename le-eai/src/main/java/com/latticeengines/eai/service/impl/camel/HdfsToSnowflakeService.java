package com.latticeengines.eai.service.impl.camel;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3Configuration;
import com.latticeengines.domain.exposed.eai.route.HdfsToSnowflakeConfiguration;
import com.latticeengines.snowflakedb.exposed.service.SnowflakeService;
import com.latticeengines.snowflakedb.exposed.util.SnowflakeUtils;

@Component("hdfsToSnowflakeService")
public class HdfsToSnowflakeService {

    @Autowired
    private HdfsToS3RouteService hdfsToS3RouteService;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private SnowflakeService snowflakeService;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Autowired
    private Configuration yarnConfiguration;

    public void uploadToS3(HdfsToSnowflakeConfiguration configuration) {
        HdfsToS3Configuration s3Configuration = new HdfsToS3Configuration();
        s3Configuration.setSplitSize(100L * 1024 * 1024);
        s3Configuration.setS3Bucket(s3Bucket);
        s3Configuration.setS3Prefix(s3Prefix(configuration));
        s3Configuration.setHdfsPath(configuration.getHdfsGlob());
        s3Configuration.setTargetFilename(s3FileName(configuration));

        if (!configuration.isAppend()) {
            cleanupS3(configuration);
        }

        hdfsToS3RouteService.downloadToLocal(s3Configuration);
        hdfsToS3RouteService.upload(s3Configuration);
    }

    public void copyToSnowflake(HdfsToSnowflakeConfiguration configuration) {
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, configuration.getHdfsGlob());
        snowflakeService.createDatabase(configuration.getDb(), s3Bucket);
        snowflakeService.createAvroTable(configuration.getDb(), configuration.getTableName(), schema,
                !configuration.isAppend(), null);
        snowflakeService.loadAvroTableFromS3(configuration.getDb(), configuration.getTableName(),
                configuration.getTableName());
    }

    // should only be used for testing purpose
    public void setS3Bucket(String bucket) {
        s3Bucket = bucket;
    }

    public void cleanupS3(HdfsToSnowflakeConfiguration configuration) {
        String prefix = s3Prefix(configuration);
        s3Service.cleanupPrefix(s3Bucket, prefix);
    }

    private String s3FileName(HdfsToSnowflakeConfiguration configuration) {
        return configuration.getTableName().toLowerCase() + ".avro";
    }

    private String s3Prefix(HdfsToSnowflakeConfiguration configuration) {
        return SnowflakeUtils.AVRO_STAGE + "/" + configuration.getTableName();
    }
}
