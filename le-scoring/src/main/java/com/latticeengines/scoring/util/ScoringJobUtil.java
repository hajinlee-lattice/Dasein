package com.latticeengines.scoring.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class ScoringJobUtil {

    public static List<String> findModelUrlsToLocalize(Configuration yarnConfiguration, String tenant,
            String customerBaseDir, List<String> modelGuids) {
        List<String> modelFilePaths = findAllModelPathsInHdfs(yarnConfiguration, tenant, customerBaseDir);
        return findModelUrlsToLocalize(yarnConfiguration, tenant, modelGuids, modelFilePaths);
    }

    public static List<String> findAllModelPathsInHdfs(Configuration yarnConfiguration, String tenant,
            String customerBaseDir) {
        String customerModelPath = customerBaseDir + "/" + tenant + "/models";
        List<String> modelFilePaths = Collections.emptyList();
        try {
            modelFilePaths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerModelPath,
                    new HdfsFileFilter() {
                        @Override
                        public boolean accept(FileStatus fileStatus) {
                            if (fileStatus == null) {
                                return false;
                            }
                            Pattern p = Pattern.compile(".*model" + ScoringDaemonService.JSON_SUFFIX);
                            Matcher matcher = p.matcher(fileStatus.getPath().getName());
                            return matcher.matches();
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException("Customer " + tenant + "'s scoring job failed due to: " + e.getMessage(), e);
        }
        if (CollectionUtils.isEmpty(modelFilePaths)) {
            throw new LedpException(LedpCode.LEDP_20008, new String[] { tenant });
        }
        return modelFilePaths;
    }

    @VisibleForTesting
    static List<String> findModelUrlsToLocalize(Configuration yarnConfiguration, String tenant,
            List<String> modelGuids, List<String> modelFilePaths) {
        List<String> modelUrlsToLocalize = new ArrayList<>();
        label: for (String modelGuid : modelGuids) {
            String uuid = UuidUtils.extractUuid(modelGuid);
            for (String path : modelFilePaths) {
                if (uuid.equals(UuidUtils.parseUuid(path))) {
                    try {
                        HdfsUtils.getCheckSum(yarnConfiguration, path);
                    } catch (IOException e) {
                        throw new LedpException(LedpCode.LEDP_20021, new String[] { path, tenant });
                    }
                    modelUrlsToLocalize.add(path + "#" + uuid);
                    continue label;
                }
            }
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelGuid });
        }
        return modelUrlsToLocalize;
    }

    public static void generateDataTypeSchema(Schema schema, String dataTypeFilePath, Configuration config) {
        List<Field> fields = schema.getFields();
        ObjectNode jsonObj = new ObjectMapper().createObjectNode();
        for (Field field : fields) {
            String type = field.schema().getTypes().get(0).getName();
            if (type.equals("string") || type.equals("bytes"))
                jsonObj.put(field.name(), 1);
            else
                jsonObj.put(field.name(), 0);
        }
        try {
            HdfsUtils.writeToFile(config, dataTypeFilePath, jsonObj.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static URI[] getURIs(String hdfsPaths) throws URISyntaxException {
        String[] paths = hdfsPaths.split(ScoringDaemonService.COMMA);
        URI[] files = new URI[paths.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new URI(paths[i].trim());
        }
        return files;
    }
}
