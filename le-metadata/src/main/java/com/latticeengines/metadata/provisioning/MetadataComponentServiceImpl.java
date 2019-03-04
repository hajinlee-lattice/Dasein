package com.latticeengines.metadata.provisioning;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.component.exposed.service.ComponentServiceBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.InstallDocument;
import com.latticeengines.metadata.service.MetadataProvisioningService;

@Component("metadataComponentService")
public class MetadataComponentServiceImpl extends ComponentServiceBase {

    private static final Logger log = LoggerFactory.getLogger(MetadataComponentServiceImpl.class);

    @Inject
    private MetadataProvisioningService metadataProvisioningService;

    public MetadataComponentServiceImpl() {
        super(ComponentConstants.METADATA);
    }

    @Override
    public boolean install(String customerSpace, InstallDocument installDocument) {
        log.info("Start install Metadata component: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            metadataProvisioningService.provisionImportTables(cs);
        } catch (Exception e) {
            log.error("Provision Metadata service failed! " + e.toString());
            return false;
        }
        log.info(String.format("Install Metadata component: %s succeeded!", customerSpace));
        return true;
    }

    @Override
    public boolean update() {
        return false;
    }

    @Override
    public boolean destroy(String customerSpace) {
        log.info("Start uninstall Metadata component for: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            metadataProvisioningService.removeImportTables(cs);
        } catch (Exception e) {
            log.error(String.format("Uninstall component for: %s failed. %s", customerSpace, e.toString()));
            return false;
        }
        return true;
    }

    @Override
    public boolean reset(String customerSpace) {
        log.info(String.format("Start reset Metadata component for: %s.", customerSpace));
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            metadataProvisioningService.removeImportTables(cs);

            Thread.sleep(1000);
            metadataProvisioningService.provisionImportTables(cs);
        } catch (Exception e) {
            log.error(String.format("Reset Metadata component for: %s failed. %s", customerSpace, e.toString()));
            return false;
        }

        log.info(String.format("Reset Metadata component for: %s succeed.", customerSpace));
        return true;
    }
}
