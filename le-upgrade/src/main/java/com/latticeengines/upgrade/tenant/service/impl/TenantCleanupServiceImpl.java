package com.latticeengines.upgrade.tenant.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.UpgradeRunner;
import com.latticeengines.upgrade.pls.PlsGaManager;
import com.latticeengines.upgrade.tenant.service.TenantUpgradeService;

@Component("tenantCleaner")
public class TenantCleanupServiceImpl implements TenantUpgradeService {

    @Autowired
    private PlsGaManager plsGaManager;

    private void deleteSingularIdPLSTenant(String customer) {
        System.out.println(String.format("\nThe old customer being deleted from PLS ... ... ... ... %s", customer));

        System.out.print("Deleting singular ID tenant in PLS/GA ... ");
        plsGaManager.deleteTenantWithSingularId(customer);
        System.out.println("OK");
    }

    private void upgrade(String customer) {
        deleteSingularIdPLSTenant(customer);
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        String customer = (String) parameters.get("customer");

        switch (command) {
            case UpgradeRunner.CMD_UPGRADE:
                upgrade(customer);
                return true;
            default:
                return false;
        }

    }

}
