package com.latticeengines.pls.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ModelingHdfsUtils {

    public static String findModelSummaryPath(Configuration config, String dir) throws IOException {
        List<String> paths = HdfsUtils.getFilesForDirRecursive(config, dir, new HdfsUtils.HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }
                String name = file.getPath().getName().toString();
                return name.equals("modelsummary.json");
            }

        });
        if (paths.size() == 0) {
            throw new LedpException(LedpCode.LEDP_00002);
        }
        return paths.get(0);
    }

    public static JsonNode constructNewModel(String modelLocalPath, String modelId) throws IOException {
        String contents = FileUtils.readFileToString(new File(modelLocalPath), "UTF-8");
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);

        ObjectNode summary = (ObjectNode) json.get("Summary");
        summary.put("ModelID", modelId);
        return json;
    }

    public static String getModelFileName(Configuration conf, String path) throws IllegalArgumentException,
            IOException {
        List<String> paths = HdfsUtils.getFilesForDir(conf, path, ".*.model.json");
        if (paths.size() == 0) {
            throw new LedpException(LedpCode.LEDP_00002);
        }
        return new Path(paths.get(0)).getName();
    }

    public static String getStandardDataComposition(Configuration conf, String sourceDataDir, final String eventTableName) throws IOException{
        List<String> paths = HdfsUtils.getFilesForDirRecursive(conf, sourceDataDir, new HdfsFileFilter() {
                    @Override
                    public boolean accept(FileStatus file) {
                       return file.getPath().getName().equals("datacomposition.json") //
                                    && file.getPath().getParent().getName().startsWith(eventTableName);
                    }
                });
        return paths.get(0);
    }
}
