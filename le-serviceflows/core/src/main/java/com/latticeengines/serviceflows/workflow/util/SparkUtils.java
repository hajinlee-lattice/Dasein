package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroParquetUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CountAvroGlobsConfig;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.spark.exposed.job.common.CountAvroGlobs;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

public final class SparkUtils {

    private static final Logger log = LoggerFactory.getLogger(SparkUtils.class);

    public static Table hdfsUnitToTable(String tableName, String primaryKey, HdfsDataUnit hdfsDataUnit, //
                                        Configuration yarnConfiguration, //
                                        String podId, CustomerSpace customerSpace) {
        String srcPath = hdfsDataUnit.getPath();
        String tgtPath = PathBuilder.buildDataTablePath(podId, customerSpace).append(tableName).toString();
        try {
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
}
