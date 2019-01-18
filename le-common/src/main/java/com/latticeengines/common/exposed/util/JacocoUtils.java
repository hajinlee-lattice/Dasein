package com.latticeengines.common.exposed.util;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JacocoUtils {

    private static final Logger log = LoggerFactory.getLogger(JacocoUtils.class);

    public static void setJacoco(Properties containerProperties, String execFile) {
        String jacocoDataDir = PropertyUtils.getProperty("yarn.jacoco.data.dir");
        boolean jacocoEnabled = Boolean.valueOf(PropertyUtils.getProperty("yarn.jacoco.enabled"));
        if (jacocoEnabled) {
            String jacocoAgentFile = "jacocoagent.jar";
            String jacocoDestFile = String.format("%s/%s.exec", jacocoDataDir, execFile);
            containerProperties.put("JACOCO_AGENT_FILE", jacocoAgentFile);
            containerProperties.put("JACOCO_DEST_FILE", jacocoDestFile);
            log.info("Using jacoco agent " + jacocoAgentFile);
            log.info("Using jacoco dest file " + jacocoDestFile);
        }
    }

    public static boolean needToLocalizeJacoco() {
        return Boolean.valueOf(PropertyUtils.getProperty("yarn.jacoco.enabled"));
    }

}
