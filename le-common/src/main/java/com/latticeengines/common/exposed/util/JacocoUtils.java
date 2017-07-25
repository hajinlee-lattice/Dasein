package com.latticeengines.common.exposed.util;

import java.io.File;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JacocoUtils {

    private static final Logger log = LoggerFactory.getLogger(JacocoUtils.class);

    public static void setJacoco(Properties containerProperties, String execFile) {
        String jacocoDataDir = PropertyUtils.getProperty("yarn.jacoco.data.dir");
        boolean jacocoEnabled = Boolean.valueOf(PropertyUtils.getProperty("yarn.jacoco.enabled"));
        if (jacocoEnabled) {
            String jacocoAgentFile = System.getenv("JACOCO_AGENT_FILE");
            if (StringUtils.isBlank(jacocoAgentFile)) {
                jacocoAgentFile = "jacocoagent.jar";
            }
            String jacocoDestFile = String.format("%s/%s.exec", jacocoDataDir, execFile);
            containerProperties.put("JACOCO_AGENT_FILE", jacocoAgentFile);
            containerProperties.put("JACOCO_DEST_FILE", jacocoDestFile);
            log.info("Using jacoco agent " + jacocoAgentFile);
            log.info("Using jacoco dest file " + jacocoDestFile);
        }
    }

    public static boolean needToLocalizeJacoco() {
        boolean jacocoEnabled = Boolean.valueOf(PropertyUtils.getProperty("yarn.jacoco.enabled"));
        String agentFile = System.getenv("JACOCO_AGENT_FILE");
        boolean envVarValid = StringUtils.isNotBlank(agentFile) && new File(agentFile).exists();
        return jacocoEnabled && !envVarValid;
    }

}
