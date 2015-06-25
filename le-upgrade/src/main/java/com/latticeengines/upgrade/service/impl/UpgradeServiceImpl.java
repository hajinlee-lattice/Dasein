package com.latticeengines.upgrade.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.model.service.ModelUpgradeService;
import com.latticeengines.upgrade.service.UpgradeService;
import com.latticeengines.upgrade.tenant.service.TenantUpgradeService;

@Component("upgrader")
public class UpgradeServiceImpl implements UpgradeService {

    private ModelUpgradeService modelUpgrader;

    @Autowired
    @Qualifier("model_134_Upgrade")
    private ModelUpgradeService modelUpgrader134;

    @Autowired
    @Qualifier("model_140_Upgrade")
    private ModelUpgradeService modelUpgrader140;

    @Autowired
    private TenantUpgradeService tenantUpgrader;

    @Override
    public void switchToVersion(String version) {
        if ("1.3.4".equals(version)) {
            this.modelUpgrader = modelUpgrader134;
        } else if ( "1.4.0".equals(version) ) {
            this.modelUpgrader = modelUpgrader140;
        } else {
            throw new IllegalArgumentException("Does not support version " + version);
        }
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        System.out.println("Sending command to model upgrader ...");
        boolean handled = this.modelUpgrader.execute(command, parameters);
        if (handled) return true;

        System.out.println("Sending command to tenant upgrader ...");
        this.tenantUpgrader.execute(command, parameters);
        return true;
    }

}
