package com.latticeengines.upgrade.yarn;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("srcYarnMgr")
public class SrcYarnMgr extends YarnManager {

    @Autowired
    @Qualifier(value = "src")
    private Configuration srcYarnConfig;

    @Value("${upgarde.src.dp.customer.basedir}")
    protected String srcCustomerBase;

    @PostConstruct
    private void wireUpProperties() {
        this.yarnConfiguration = this.srcYarnConfig;
        this.customerBase = srcCustomerBase;
    }


}
