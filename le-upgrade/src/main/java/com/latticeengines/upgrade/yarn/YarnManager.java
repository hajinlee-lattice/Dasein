package com.latticeengines.upgrade.yarn;

import org.apache.hadoop.conf.Configuration;

public class YarnManager {

    protected Configuration yarnConfiguration;

    protected String customerBase;

    public String defaultFs() {
        return yarnConfiguration.get("fs.defaultFS");
    }

}
