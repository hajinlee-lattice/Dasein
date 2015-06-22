package com.latticeengines.upgrade.yarn;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DestYarnMgr extends YarnManager {

    @Autowired
    @Qualifier(value = "dest")
    private Configuration destYarnConfig;

    @Value("${dataplatform.customer.basedir}")
    private String destCustomerBase;

    @PostConstruct
    private void wireUpProperties() {
        this.yarnConfiguration = this.destYarnConfig;
        this.customerBase = this.destCustomerBase;
    }

    public String constructModelDir(String customer, String uuid) throws Exception {
        String modelDir = customerBase + "/" + customer + "/models" + uuid;
        return modelDir;
    }
}
