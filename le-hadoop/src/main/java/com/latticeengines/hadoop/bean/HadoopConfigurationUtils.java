package com.latticeengines.hadoop.bean;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopConfigurationUtils {

    private static final Logger log = LoggerFactory.getLogger(HadoopConfigurationUtils.class);

    private static final String TOKEN = "${MASTER_IP}";

    public static Properties loadPropsFromResource(String resource, String masterIp) {
        Properties properties = new Properties();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        try {
            properties.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + resource, e);
        }
        properties.forEach((key1, value) -> {
            String key = (String) key1;
            String val = String.valueOf(value);
            if (StringUtils.isNotBlank(val) && val.contains(TOKEN)) {
                val = val.replace(TOKEN, masterIp);
                properties.setProperty(key, val);
                log.info(String.format("Set %s=%s", key, val));
            }
        });
        return properties;
    }

}
