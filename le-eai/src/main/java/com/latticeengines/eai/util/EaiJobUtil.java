package com.latticeengines.eai.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.HdfsUtils;

public class EaiJobUtil {

    public static List<String> getCacheFiles(Configuration yarnConfiguration, String currentVersionInStack)
            throws IOException {
        String dependencyPath = "/app/";
        String jarDependencyPath = "/eai/lib";
        String log4jXmlPath = "/conf/log4j2-yarn.xml";
        List<String> jarFilePaths = HdfsUtils.getFilesForDir(yarnConfiguration,
                dependencyPath + currentVersionInStack + jarDependencyPath, ".*\\.jar$");
        jarFilePaths.add(dependencyPath + currentVersionInStack + log4jXmlPath);
        return jarFilePaths;
    }
}
