package com.latticeengines.admin.tenant.batonadapter.modeling;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ModelingComponentManager {

    private static final Logger log = LoggerFactory.getLogger(ModelingComponentManager.class);

    private static final String HDFS_POD_PATH = "/Pods/%s/Contracts/%s";

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${admin.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    public void cleanupHdfs(CustomerSpace space) {
        try {
            String modelingHdfsPoint = modelingServiceHdfsBaseDir + "/" + space.toString();
            HdfsUtils.rmdir(yarnConfiguration, modelingHdfsPoint);
            String podHdfsPoint = String.format(HDFS_POD_PATH, CamilleEnvironment.getPodId(), space.getContractId());
            HdfsUtils.rmdir(yarnConfiguration, podHdfsPoint);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

}
