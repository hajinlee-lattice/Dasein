package com.latticeengines.upgrade.model.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DestYarnMgr {

    @Autowired
    @Qualifier(value = "dest")
    private Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    public String defaultFs() {
        return yarnConfiguration.get("fs.defaultFS");
    }

    public String constructModelDir(String customer, String uuid) throws Exception {
        String modelDir = customerBase + "/" + customer + "/models" + uuid;
        return modelDir;
    }
}
