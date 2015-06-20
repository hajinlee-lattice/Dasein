package com.latticeengines.upgrade.model.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class SrcYarnMgr {

    @Autowired
    @Qualifier(value = "src")
    private Configuration yarnConfiguration;

    public String defaultFs() {
        return yarnConfiguration.get("fs.defaultFS");
    }
}
