package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroParquetUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.LivyScalingConfig;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CountAvroGlobsConfig;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.serviceflows.workflow.dataflow.LivySessionManager;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.common.CountAvroGlobs;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

public final class SparkUtils {

    protected SparkUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(SparkUtils.class);

    public static Table hdfsUnitToTable(String tableName, String primaryKey, HdfsDataUnit hdfsDataUnit, //
                                        Configuration yarnConfiguration, //
                                        String podId, CustomerSpace customerSpace) {
        String srcPath = hdfsDataUnit.getPath();
        String tgtPath = PathBuilder.buildDataTablePath(podId, customerSpace).append(tableName).toString();
        try {
            log.info("Moving file from {} to {}", srcPath, tgtPath);
            HdfsUtils.moveFile(yarnConfiguration, srcPath, tgtPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to move data from " + srcPath + " to " + tgtPath);
        }

        Table table;
        if (DataUnit.DataFormat.PARQUET.equals(hdfsDataUnit.getDataFormat())) {
            table = MetadataConverter.getParquetTable(yarnConfiguration, tgtPath, //
                    primaryKey, null, true);
        } else {
            table = MetadataConverter.getTable(yarnConfiguration, tgtPath, //
                    primaryKey, null, true);
        }
        table.setName(tableName);
        if (hdfsDataUnit.getCount() != null) {
            table.getExtracts().get(0).setProcessedRecords(hdfsDataUnit.getCount());
        }

        return table;
    }

    public static S3DataUnit hdfsUnitToS3DataUnit(HdfsDataUnit hdfsDataUnit, Configuration yarnConfiguration, BusinessEntity entity,
                                                  String podId, CustomerSpace customerSpace, String bucket, String templateId, List<DataUnit.Role> roles) {
        String tenantId = customerSpace.getTenantId();
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        S3DataUnit s3DataUnit = new S3DataUnit();
        String dataUnitName = NamingUtils.uuid(entity.name());
        String srcPath = hdfsDataUnit.getPath();
        String tgtPath = pathBuilder.getHdfsAtlasDataUnitPrefix(podId, tenantId, templateId, dataUnitName);
        try {
            for (String avroParquetPath : AvroParquetUtils.listAvroParquetFiles(yarnConfiguration, srcPath, false)) {
                if (!HdfsUtils.isDirectory(yarnConfiguration, tgtPath)) {
                    HdfsUtils.mkdir(yarnConfiguration, tgtPath);
                }
                Path dstPath = new Path(avroParquetPath.replace(srcPath, tgtPath));
                Path parent = dstPath.getParent();
                if (parent != null && !HdfsUtils.fileExists(yarnConfiguration, parent.toString())) {
                    HdfsUtils.mkdir(yarnConfiguration, parent.toString());
                }
                log.info("Moving file from {} to {}", avroParquetPath, dstPath.toString());
                HdfsUtils.moveFile(yarnConfiguration, avroParquetPath, dstPath.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to move data from " + srcPath + " to " + tgtPath);
        }
        s3DataUnit.setTenant(tenantId);
        s3DataUnit.setName(dataUnitName);
        s3DataUnit.setCount(hdfsDataUnit.getCount());
        s3DataUnit.setLinkedHdfsPath(tgtPath);
        s3DataUnit.setRoles(roles);
        s3DataUnit.setBucket(bucket);
        s3DataUnit.setDataFormat(hdfsDataUnit.getDataFormat());
        s3DataUnit.setDataTemplateId(templateId);
        String key = pathBuilder.getS3AtlasDataUnitPrefix(bucket, tenantId, templateId, dataUnitName);
        key = key.substring(key.indexOf(bucket) + bucket.length() + 1);
        s3DataUnit.setPrefix(key);
        return s3DataUnit;
    }

    /*
     * copy entire directory and preserve directory structure instead of using glob
     */
    public static Table hdfsUnitDirToTable(String tableName, String primaryKey, HdfsDataUnit hdfsDataUnit, //
                                           Configuration yarnConfiguration, //
                                           String podId, CustomerSpace customerSpace) {
        String srcPath = hdfsDataUnit.getPath();
        String tgtPath = PathBuilder.buildDataTablePath(podId, customerSpace).append(tableName).toString();
        try {
            // make sure target path dir exists
            if (!HdfsUtils.fileExists(yarnConfiguration, tgtPath)) {
                HdfsUtils.mkdir(yarnConfiguration, tgtPath);
            }

            int nCopied = copyAvroParquetFiles(yarnConfiguration, srcPath, tgtPath);
            Preconditions.checkArgument(nCopied > 0,
                    String.format("No avro/parquet files copied. src=%s, tgt=%s", srcPath, tgtPath));
        } catch (IOException e) {
            String msg = String.format("Failed to move data from %s to %s", srcPath, tgtPath);
            throw new RuntimeException(msg, e);
        }

        Table table = MetadataConverter.getTableFromDir(yarnConfiguration, tgtPath, primaryKey, null, true);
        table.setName(tableName);
        if (hdfsDataUnit.getCount() != null) {
            table.getExtracts().get(0).setProcessedRecords(hdfsDataUnit.getCount());
        }

        return table;
    }

    // return copied file count
    private static int copyAvroParquetFiles(@NotNull Configuration yarnConfiguration, @NotNull String srcDir,
                                            @NotNull String dstDir) throws IOException {
        int fileCnt = 0;
        for (String avroParquetPath : AvroParquetUtils.listAvroParquetFiles(yarnConfiguration, srcDir, false)) {
            if (!HdfsUtils.isDirectory(yarnConfiguration, dstDir)) {
                HdfsUtils.mkdir(yarnConfiguration, dstDir);
            }
            Path dstPath = new Path(avroParquetPath.replace(srcDir, dstDir));
            Path parent = dstPath.getParent();
            if (parent != null && !HdfsUtils.fileExists(yarnConfiguration, parent.toString())) {
                HdfsUtils.mkdir(yarnConfiguration, parent.toString());
            }
            HdfsUtils.moveFile(yarnConfiguration, avroParquetPath, dstPath.toString());
            fileCnt++;
        }
        return fileCnt;
    }

    public static int moveAvroParquetFiles(@NotNull Configuration yarnConfiguration, @NotNull String srcDir,
            @NotNull String dstDir, @NotNull String prefix) throws IOException {
        int fileCnt = 0;
        for (String avroParquetPath : AvroParquetUtils.listAvroParquetFiles(yarnConfiguration, srcDir, false)) {
            if (!HdfsUtils.isDirectory(yarnConfiguration, dstDir)) {
                HdfsUtils.mkdir(yarnConfiguration, dstDir);
            }
            Path tgtPath = new Path(avroParquetPath.replace(srcDir, dstDir));
            String dstName = tgtPath.getParent().toString() + "/" + prefix + tgtPath.getName();
            Path dstPath = new Path(dstName);
            Path parent = dstPath.getParent();
            if (parent != null && !HdfsUtils.fileExists(yarnConfiguration, parent.toString())) {
                HdfsUtils.mkdir(yarnConfiguration, parent.toString());
            }
            HdfsUtils.moveFile(yarnConfiguration, avroParquetPath, dstPath.toString());
            fileCnt++;
        }
        return fileCnt;
    }

    public static Long countRecordsInGlobs(LivySessionService sessionService, SparkJobService sparkJobService,
                                           Configuration yarnConfig, String... globs) {
        if (globs[0].endsWith(".parquet")) { // assuming all paths in the array have the same file type
            return ParquetUtils.countParquetFiles(yarnConfig, globs);
        } else {
            if (shouldCountWithAvroUtils(yarnConfig, globs)) {
                return AvroUtils.count(yarnConfig, globs);
            }
            LivySession session = sessionService.startSession(CountAvroGlobs.class.getSimpleName(),
                    Collections.emptyMap(), Collections.emptyMap());
            CountAvroGlobsConfig config = new CountAvroGlobsConfig();
            config.avroGlobs = globs;
            SparkJobResult result = sparkJobService.runJob(session, CountAvroGlobs.class, config);
            sessionService.stopSession(session);
            return Long.parseLong(result.getOutput());
        }
    }

    private static Boolean shouldCountWithAvroUtils(Configuration yarnConfig, String... globs) {
        // if total size of all files less than 1 GB, count with AvroUtils
        return (getGlobsSize(yarnConfig, globs) < 1.);
    }

    private static double getGlobsSize(Configuration yarnConfig, String... globs) {
        double size = .0;
        for (String glob : globs) {
            size += ScalingUtils.getHdfsPathSizeInGb(yarnConfig, glob);
        }
        return size;
    }

    public static <J extends AbstractSparkJob<C>, C extends SparkJobConfig> SparkJobResult
    runJob(CustomerSpace customerSpace, Configuration yarnConfiguration, SparkJobService sparkJobService,
           LivySessionManager livySessionManager, Class<J> jobClz, C jobConfig) {
        double totalSizeInGb = SparkUtils.getGlobsSize(yarnConfiguration,
                jobConfig.getInput().stream().map(hdfsDataUnit -> ((HdfsDataUnit) hdfsDataUnit).getPath()).toArray(String[]::new));
        int scalingMultiplier = ScalingUtils.getMultiplier(totalSizeInGb);
        try {
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            SparkJobResult sparkJobResult = retry.execute(context -> {
                if (context.getRetryCount() > 0) {
                    log.info("Attempt=" + (context.getRetryCount() + 1) + ": retry running spark job " //
                            + jobClz.getSimpleName());
                    log.warn("Previous failure:", context.getLastThrowable());
                    livySessionManager.killSession();
                }
                String jobName = customerSpace.getTenantId() + "~" + jobClz.getSimpleName();
                LivySession session = livySessionManager.createLivySession(jobName, new LivyScalingConfig(scalingMultiplier, 1), null);
                return sparkJobService.runJob(session, jobClz, jobConfig);
            });
            return sparkJobResult;
        } finally {
            livySessionManager.killSession();
        }
    }
}
