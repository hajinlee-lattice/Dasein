package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;

public class AvroParquetUtils {

    public static Schema parseAvroSchema(Configuration configuration, String globPath) {
        globPath = toParquetOrAvroGlob(configuration, globPath);
        if (globPath.endsWith(".parquet")) {
            return ParquetUtils.getAvroSchema(configuration, globPath);
        } else {
            return AvroUtils.getSchemaFromGlob(configuration, globPath);
        }
    }

    public static long countParquetOrAvro(Configuration configuration, String... globPaths) {
        if (isParquet(configuration, globPaths)) {
            return ParquetUtils.countParquetFiles(configuration, globPaths);
        } else {
            return AvroUtils.count(configuration, globPaths);
        }
    }

    public static String toParquetOrAvroGlob(Configuration configuration, String path) {
        // use hdfs util check if exist any parquet file
        // if so, return parquet glob, otherwise return avro glob
        try {
            String parquetGlob = PathUtils.toParquetGlob(path);
            if (CollectionUtils.isNotEmpty(HdfsUtils.getFilesByGlob(configuration, parquetGlob))) {
                return parquetGlob;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check if parquet file exists", e);
        }
        return PathUtils.toAvroGlob(path);
    }

    private static boolean isParquet(Configuration configuration, String... globPaths) {
        for (String globPath: globPaths) {
            List<String> files;
            try {
                files = HdfsUtils.getFilesByGlob(configuration, globPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to resolve glob " + globPath);
            }
            if (CollectionUtils.isNotEmpty(files)) {
                for (String file: files) {
                    if (file.endsWith(".parquet")) {
                        return true;
                    } else if (file.endsWith(".avro")) {
                        return false;
                    }
                }
            }
        }
        return false;
    }

}
