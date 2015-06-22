package com.latticeengines.upgrade.yarn;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Component("destYarnMgr")
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

    public String constructTupleIdModelDir(String dlTenantName, String modelGuid) throws Exception {
        String tupleId = CustomerSpace.parse(dlTenantName).toString();
        String uuid = StringUtils.remove(modelGuid, "ms__").substring(0, 36);
        String modelDir = customerBase + "/" + tupleId + "/models/" + UPGRADE_EVENT_TABLE + "/" + uuid
                + "/" + UPGRADE_CONTAINER_ID + "/";
        return modelDir;
    }

    private void uploadModelToHdfs(String dlTenantName, String modelGuid, String modelContent) throws Exception{
        String path = constructTupleIdModelDir(dlTenantName, modelGuid) + "model.json";
        HdfsUtils.writeToFile(yarnConfiguration, path, modelContent);
    }
}
