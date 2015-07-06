package com.latticeengines.upgrade.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.model.service.ModelUpgradeService;
import com.latticeengines.upgrade.service.UpgradeService;
import com.latticeengines.upgrade.tenant.service.TenantUpgradeService;

@Component("upgrader")
public class UpgradeServiceImpl implements UpgradeService {

    @Autowired
    private ModelUpgradeService modelUpgrader;

    @Autowired
    private TenantUpgradeService tenantUpgrader;

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        System.out.println("Sending command to tenant upgrader ...");
        this.tenantUpgrader.execute(command, parameters);
        System.out.println("\nDone.");

        System.out.println("\n\nSending command to model upgrader ...");
        this.modelUpgrader.execute(command, parameters);
        System.out.println("\nDone.");

        return true;
    }

}
