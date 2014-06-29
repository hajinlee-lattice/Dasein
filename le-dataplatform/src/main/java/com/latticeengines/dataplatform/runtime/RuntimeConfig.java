package com.latticeengines.dataplatform.runtime;

import java.io.StringWriter;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;

public class RuntimeConfig {

    private final static Log log = LogFactory.getLog(RuntimeConfig.class);

    private Properties runtimeConfig;

    private Configuration yarnConfiguration;

    private String configPath;

    public RuntimeConfig(Properties runtimeConfig, String configPath, Configuration yarnConfiguration) {
        this.runtimeConfig = runtimeConfig;
        this.configPath = configPath;
        this.yarnConfiguration = yarnConfiguration;
    }

    public RuntimeConfig(String configPath, Configuration yarnConfiguration) {
        this(new Properties(), configPath, yarnConfiguration);
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public Properties getRuntimeConfig() {
        return runtimeConfig;
    }

    public void setRuntimeConfig(Properties runtimeConfig) {
        this.runtimeConfig = runtimeConfig;
    }

    public void addProperties(String key, String value) {
        runtimeConfig.put(key, value);
    }

    public String getProperties(String key) {
        return runtimeConfig.getProperty(key);
    }

    public void writeToHdfs() throws LedpException {
        if (runtimeConfig == null) {
            log.warn("No runtime configuration to write");
            return;
        }

        try {
            StringWriter sw = new StringWriter();
            runtimeConfig.store(sw, null);
            String properties = sw.toString();
            log.info("Writing runtime config as:\n" + properties);
            HdfsUtils.writeToFile(yarnConfiguration, configPath, properties);
        } catch (Exception e) {
            log.error("Could not write runtime configuration due to: " + e);
            throw new LedpException(LedpCode.LEDP_12001, e, new String[] { configPath });
        }
    }
}
