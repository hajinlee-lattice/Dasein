package com.latticeengines.sqoop.util;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnConfigurationUtils {

    private static final Logger log = LoggerFactory.getLogger(YarnConfigurationUtils.class);

    public static YarnConfiguration getYarnConfiguration() {
        YarnConfiguration configuration = new YarnConfiguration();
        String defaultFs = configuration.get("fs.defaultFS");
        if (defaultFs.startsWith("hdfs://ip-")) {
            final String address = defaultFs.substring("hdfs://".length(), defaultFs.indexOf(":"));
            final String ip = address.substring("ip-".length(), address.indexOf(".")).replace("-", ".");
            log.info("Change address from " + address + " to " + ip);
            configuration.forEach(entry -> {
                String key = entry.getKey();
                String val = entry.getValue();
                if (val.contains(address)) {
                    String newVal = val.replace(address, ip);
                    log.info("Changing " + key + " from " + val + " to " + newVal);
                    configuration.set(key, newVal);
                }
            });
        }
        return configuration;
    }

}
