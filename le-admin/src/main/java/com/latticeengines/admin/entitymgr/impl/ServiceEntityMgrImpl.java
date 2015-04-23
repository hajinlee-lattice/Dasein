package com.latticeengines.admin.entitymgr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.ServiceEntityMgr;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

@Component("serviceEntityMgr")
public class ServiceEntityMgrImpl implements ServiceEntityMgr {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ServiceEntityMgrImpl.class);

    private final BatonService batonService = new BatonServiceImpl();

    @Override
    public SerializableDocumentDirectory getDefaultServiceConfig(String serviceName) {
        DocumentDirectory dir = batonService.getDefaultConfiguration(serviceName);
        if (dir != null) {
            SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(dir);
            DocumentDirectory metaDir = batonService.getConfigurationSchema(serviceName);
            sDir.applyMetadata(metaDir);
            return sDir;
        }
        return null;
    }

    @Override
    public DocumentDirectory getConfigurationSchema(String serviceName) {
        return batonService.getConfigurationSchema(serviceName);
    }
}
