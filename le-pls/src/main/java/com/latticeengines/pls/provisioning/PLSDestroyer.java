package com.latticeengines.pls.provisioning;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.security.exposed.service.TenantService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;

public class PLSDestroyer implements CustomerSpaceServiceDestroyer {

    private static final Log log = LogFactory.getLog(PLSDestroyer.class);

    private PLSComponentManager componentManager;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        componentManager.discardTenant(space.toString());
        return true;
    }

    public void setComponentManager(PLSComponentManager manager) {
        this.componentManager = manager;
    }

}
