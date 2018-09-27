package com.latticeengines.hadoop.bean;

import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.aws.emr.EMRService;

public class EMRConfigurationFactory {

    private static final Logger log = LoggerFactory.getLogger(EMRConfigurationFactory.class);

    @Inject
    private EMRService emrService;

    @Value("${aws.default.access.key}")
    protected String awsKey;

    @Value("${aws.default.secret.key.encrypted}")
    protected String awsSecret;

    public YarnConfiguration getYarnConfiguration(String emrCluster) {
        String masterIp;
        if (StringUtils.isNotBlank(emrCluster)) {
            masterIp = emrService.getMasterIp(emrCluster);
        } else {
            masterIp = emrService.getMasterIp();
        }

        Properties properties = HadoopConfigurationUtils.loadPropsFromResource("emr.properties", //
                masterIp, awsKey, awsSecret);
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        properties.forEach((k, v) -> yarnConfiguration.set((String) k, (String) v));
        String fs = yarnConfiguration.get("fs.defaultFS");
        log.info(String.format("Created a YarnConfiguration (%d): %s", //
                System.identityHashCode(yarnConfiguration), fs));
        return yarnConfiguration;
    }

}
