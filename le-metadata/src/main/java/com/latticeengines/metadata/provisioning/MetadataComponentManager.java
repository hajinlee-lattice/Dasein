package com.latticeengines.metadata.provisioning;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.MetadataProvisioningService;

@Component
public class MetadataComponentManager {

    private static final Logger log = LoggerFactory.getLogger(MetadataComponentManager.class);

    @Inject
    private MetadataProvisioningService metadataProvisioningService;

    public void provisionImportTables(CustomerSpace space, DocumentDirectory configDir) {

        log.info(String.format("Provisioning tenant %s", space.toString()));
        metadataProvisioningService.provisionImportTables(space);
    }

    public void removeImportTables(CustomerSpace space) {
        log.info(String.format("Removing tenant %s", space.toString()));
        metadataProvisioningService.removeImportTables(space);
    }

    public void purgeData(CustomerSpace space) {
        log.info(String.format("Removing data in tenant %s", space.toString()));
        Tenant tenant = new Tenant();
        tenant.setId(space.toString());
        tenant.setName(space.getTenantId());
    }
}
