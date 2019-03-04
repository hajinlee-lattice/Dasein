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
    private static final String AWS_ACCESS_KEY_ID = "${AWS_ACCESS_KEY_ID}";
    private static final String AWS_SECRET_ACCESS_KEY = "${AWS_SECRET_ACCESS_KEY}";

    public static Properties loadPropsFromResource(String resource, String masterIp, String awsKey, String awsSecret) {
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
            if (StringUtils.isNotBlank(val)){
                String val2 = val;
                if (val.contains(AWS_ACCESS_KEY_ID)) {
                    val2 = val2.replace(AWS_ACCESS_KEY_ID, awsKey);
                }
                if (val.contains(AWS_SECRET_ACCESS_KEY)) {
                    val2 = val2.replace(AWS_SECRET_ACCESS_KEY, awsSecret);
                }
                if  (val.contains(TOKEN)) {
                    val2 = val2.replace(TOKEN, masterIp);
                }
                if (!val.equals(val2)) {
                    properties.setProperty(key, val2);
                }
            }
        });
        return properties;
    }

}
