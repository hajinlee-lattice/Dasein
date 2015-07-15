package com.latticeengines.upgrade.tenant.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.upgrade.UpgradeRunner;
import com.latticeengines.upgrade.dl.DataLoaderManager;
import com.latticeengines.upgrade.jdbc.GaJdbcManager;
import com.latticeengines.upgrade.jdbc.PlsMultiTenantJdbcManager;
import com.latticeengines.upgrade.pls.PlsGaManager;
import com.latticeengines.upgrade.tenant.service.TenantUpgradeService;
import com.latticeengines.upgrade.zk.ZooKeeperManager;

@Component("tenantUpgrader")
public class TenantUpgradeServiceImpl implements TenantUpgradeService {

    @Autowired
    private DataLoaderManager dataLoaderManager;

    @Autowired
    private ZooKeeperManager zooKeeperManager;

    @Autowired
    private PlsGaManager plsGaManager;

    @Autowired
    private PlsMultiTenantJdbcManager plsMultiTenantJdbcManager;

    @Autowired
    private GaJdbcManager gaJdbcManager;

    private void registerCustomerInZK(String customer) {
        System.out.println(String.format("\nThe customer being registered in ZK ... ... ... ... %s", customer));

        System.out.print("Creating tenant in ZK ... ");
        zooKeeperManager.registerTenantIfNotExist(customer);
        System.out.println("OK");

        SpaceConfiguration spaceConfiguration = fetchSpaceConfigurationFromDL(customer);

        System.out.print("Upload space configuration to ZK ... ");
        zooKeeperManager.uploadSpaceConfiguration(customer, spaceConfiguration);
        System.out.println("OK");

        System.out.print("Upload CRM credentials to ZK ... ");
        zooKeeperManager.uploadCrmCredentials(customer, spaceConfiguration.getDlAddress(),
                spaceConfiguration.getTopology());
        System.out.println("OK");

        System.out.print("Set Bootstrap stat to MIGRATED ... ");
        zooKeeperManager.setBootstrapStateToMigrate(customer);
        System.out.println("OK");
    }

    private void registerCustomerInPLS(String customer) {
        System.out.println(String.format("\nThe customer being registered in PLS ... ... ... ... %s", customer));

        System.out.print("Check if tenant already in PLS and GA ... ");
        boolean inpls = plsMultiTenantJdbcManager.hasTenantId(customer);
        boolean inqa = gaJdbcManager.hasDeploymentId(customer);
        if (inpls != inqa) {
            throw new RuntimeException("The records in PLS_MultiTenant and Global_Authentication are not in sync.");
        }
        System.out.println(inpls ? "YES": "NO");

        if (inpls) {
            System.out.print("Change tenantId in PLS and GA ... ");
            plsMultiTenantJdbcManager.substituteSingularIdByTupleId(customer);
            gaJdbcManager.substituteSingularIdByTupleId(customer);
            System.out.println("OK");
        } else {
            System.out.print("Creating tenant in PLS ... ");
            plsGaManager.registerTenant(customer);
            System.out.println("OK");
        }

        System.out.print("Setting up admin users in PLS ... ");
        plsGaManager.setupAdminUsers(customer);
        System.out.println("OK");
    }

    private void upgrade(String customer) {
        registerCustomerInPLS(customer);
        registerCustomerInZK(customer);
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

        switch (command) {
            case (UpgradeRunner.CMD_REGISTER_ZK):
                registerCustomerInZK(customer);
                return true;
            case (UpgradeRunner.CMD_REGISTER_PLS):
                registerCustomerInPLS(customer);
                return true;
            case (UpgradeRunner.CMD_UPGRADE):
                upgrade(customer);
                return true;
            default:
                return false;
        }

    }

}
