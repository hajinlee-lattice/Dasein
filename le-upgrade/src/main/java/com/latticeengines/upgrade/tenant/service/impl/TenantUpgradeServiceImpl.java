package com.latticeengines.upgrade.tenant.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.upgrade.UpgradeRunner;
import com.latticeengines.upgrade.dl.DataLoaderManager;
import com.latticeengines.upgrade.tenant.service.TenantUpgradeService;
import com.latticeengines.upgrade.zk.ZooKeeperManager;

@Component
public class TenantUpgradeServiceImpl implements TenantUpgradeService {

    @Autowired
    private DataLoaderManager dataLoaderManager;

    @Autowired
    private ZooKeeperManager zooKeeperManager;

    private void registerCustomerInZK(String customer) {
        System.out.println(String.format("\nThe customer being registered in ZK ... ... ... ... %s", customer));

        System.out.print("Creating tenant in ZK ... ");
        zooKeeperManager.registerTenant(customer);
        System.out.println("OK");

        SpaceConfiguration spaceConfiguration = fetchSpaceConfigurationFromDL(customer);

        System.out.print("Upload space configuration to ZK ... ");
        zooKeeperManager.uploadSpaceConfiguration(customer, spaceConfiguration);
        System.out.println("OK");
    }

    private SpaceConfiguration fetchSpaceConfigurationFromDL(String customer) {
        System.out.print("Fetching info from DataLoader (this is slow) ... ");
        SpaceConfiguration spaceConfiguration = dataLoaderManager.constructSpaceConfiguration(customer);
        System.out.println("OK");
        return spaceConfiguration;
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        String customer = (String) parameters.get("customer");
        Boolean all = (Boolean) parameters.get("all");

        switch (command) {
            case (UpgradeRunner.CMD_REGISTER_ZK):
                registerCustomerInZK(customer);
                return true;
            default:
                return false;
        }

    }

}
