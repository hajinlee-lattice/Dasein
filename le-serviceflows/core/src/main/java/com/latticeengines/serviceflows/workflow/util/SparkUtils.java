package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
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
        double size = .0;

        for (String glob : globs) {
            size += ScalingUtils.getHdfsPathSizeInGb(yarnConfig, glob);
        }
        return (size < 1.);
    }
}
