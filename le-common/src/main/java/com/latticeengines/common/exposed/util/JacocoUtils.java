package com.latticeengines.common.exposed.util;

import java.io.File;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

public final class JacocoUtils {

    public static void setJacoco(Properties containerProperties, String execFile) {
        String jacocoDataDir = PropertyUtils.getProperty("yarn.jacoco.data.dir");
        boolean jacocoEnabled = Boolean.valueOf(PropertyUtils.getProperty("yarn.jacoco.enabled"));
        if (jacocoEnabled) {
            String jacocoAgentFile = System.getenv("JACOCO_AGENT_FILE");
            if (StringUtils.isBlank(jacocoAgentFile) || !new File(jacocoAgentFile).exists()) {
                jacocoAgentFile = "jacocoagent.jar";
            }
            String jacocoDestFile = String.format("%s/%s.exec", jacocoDataDir, execFile);
            containerProperties.put("JACOCO_AGENT_FILE", jacocoAgentFile);
            containerProperties.put("JACOCO_DEST_FILE", jacocoDestFile);
        }
    }

    public static boolean needToLocalizeJacoco() {
        boolean jacocoEnabled = Boolean.valueOf(PropertyUtils.getProperty("yarn.jacoco.enabled"));
        String agentFile = System.getenv("JACOCO_AGENT_FILE");
        boolean envVarValid = StringUtils.isNotBlank(agentFile) && new File(agentFile).exists();
        return jacocoEnabled && !envVarValid;
    }

}
