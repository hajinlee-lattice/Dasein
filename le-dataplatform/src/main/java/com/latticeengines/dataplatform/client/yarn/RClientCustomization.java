package com.latticeengines.dataplatform.client.yarn;

import java.util.Properties;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.client.DefaultYarnClientCustomization;

@Component("RClientCustomization")
public class RClientCustomization extends DefaultYarnClientCustomization {

    public RClientCustomization() {
        super(null, null, null, null);
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
