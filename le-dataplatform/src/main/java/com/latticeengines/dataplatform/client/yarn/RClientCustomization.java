package com.latticeengines.dataplatform.client.yarn;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public class RClientCustomization extends DefaultYarnClientCustomization {

    public RClientCustomization(Configuration yarnConfiguration) {
        super(yarnConfiguration);
    }

    @Override
    public String getClientId() {
        return "RClient";
    }

    @Override
    public String getContainerLauncherContextFile(Properties properties) {
        return "/R/dataplatform-R-appmaster-context.xml";
    }

}
