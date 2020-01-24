package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class AvroParquetUtils {

    protected AvroParquetUtils() {
        throw new UnsupportedOperationException();
    }
    private static final String PARQUET_EXTENSION = ".parquet";
    private static final String AVRO_EXTENSION = ".avro";
    private static final HdfsUtils.HdfsFileFilter AVRO_PARQUET_FILTER = file -> {
        if (file == null || file.getPath() == null) {
            return false;
        }
        String filename = file.getPath().getName();
        return filename != null && (filename.endsWith(PARQUET_EXTENSION) || filename.endsWith(AVRO_EXTENSION));
    };

    // determine schema by the first avro/parquet file found in directory and its
    // sub-directory
    public static Schema parseAvroSchemaInDirectory(Configuration configuration, String dir) {
        List<String> files = listAvroParquetFiles(configuration, dir, true);
        if (CollectionUtils.isEmpty(files)) {
            throw new IllegalArgumentException(String.format("No avro/parquet files in directory %s", dir));
        }

        String path = files.get(0);
        if (path.endsWith(PARQUET_EXTENSION)) {
            return ParquetUtils.getAvroSchema(configuration, path);
        } else {
            return AvroUtils.getSchema(configuration, new Path(path));
        }
    }

    public static List<String> listAvroParquetFiles(Configuration configuration, String dir,
            boolean onlyReturnFirstMatch) {
        try {
            return HdfsUtils.onlyGetFilesForDirRecursive(configuration, dir, AVRO_PARQUET_FILTER, onlyReturnFirstMatch);
        } catch (IOException e) {
            String msg = String.format("Failed to list avro/parquet files in directory %s", dir);
            throw new RuntimeException(msg, e);
        }
    }

    public static Schema parseAvroSchema(Configuration configuration, String globPath) {
        globPath = toParquetOrAvroGlob(configuration, globPath);
        if (globPath.endsWith(PARQUET_EXTENSION)) {
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
                    if (file.endsWith(PARQUET_EXTENSION)) {
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
