package com.latticeengines.upgrade.model.service.impl;

import java.util.Map;
import org.springframework.stereotype.Component;

@Component("model_134_Upgrade")
public class Model_134_UpgradeServiceImpl extends ModelUpgradeServiceImpl {

    private static final String VERSION = "1.3.4";

    @Override
    public void upgrade() throws Exception {
        setVersion(VERSION);
        populateTenantModelInfo();
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        System.out.println(VERSION + " upgrader is about to execute: " + command);
        this.setVersion(VERSION);
        boolean handledByParentUpgrader = super.execute(command, parameters);
        if (!handledByParentUpgrader) {
            return false;
        }
        return false;
    }

}
