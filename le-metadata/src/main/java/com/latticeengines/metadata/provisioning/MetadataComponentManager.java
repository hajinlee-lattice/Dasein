package com.latticeengines.metadata.provisioning;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.metadata.service.MetadataProvisioningService;

@Component
public class MetadataComponentManager {

    private static final Log log = LogFactory.getLog(MetadataComponentManager.class);

    @Autowired
    private MetadataProvisioningService metadataProvisioningService;

    public void provisionImportTables(CustomerSpace space, DocumentDirectory configDir) {

        log.info(String.format("Provisioning tenant %s", space.toString()));
        metadataProvisioningService.provisionImportTables(space);
    }

}
