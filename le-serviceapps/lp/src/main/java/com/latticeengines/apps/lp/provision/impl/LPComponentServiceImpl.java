package com.latticeengines.apps.lp.provision.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.provision.LPComponentManager;
import com.latticeengines.component.exposed.service.ComponentServiceBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.InstallDocument;

@Component("lpComponentService")
public class LPComponentServiceImpl extends ComponentServiceBase {

    private static final Logger log = LoggerFactory.getLogger(LPComponentServiceImpl.class);

    @Inject
    private LPComponentManager lpComponentManager;

    public LPComponentServiceImpl() {
        super(ComponentConstants.LP);
    }

    @Override
    public boolean install(String customerSpace, InstallDocument installDocument) {
        log.info("Start install LP component: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            lpComponentManager.provisionTenant(cs, installDocument);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Provision LP service failed! " + e.toString());
            return false;
        }
        log.info(String.format("Install LP component: %s succeeded!", customerSpace));
        return true;
    }

    @Override
    public boolean update() {
        return false;
    }

    @Override
    public boolean destroy(String customerSpace) {
        log.info("Start uninstall LP component for: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            lpComponentManager.discardTenant(cs.toString());
        } catch (Exception e) {
            log.error(String.format("Uninstall component LP for: %s failed. %s", customerSpace, e.toString()));
            return false;
        }
        return true;
    }
}
