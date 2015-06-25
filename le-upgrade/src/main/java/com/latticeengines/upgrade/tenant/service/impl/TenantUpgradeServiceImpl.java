package com.latticeengines.upgrade.tenant.service.impl;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.tenant.service.TenantUpgradeService;

@Component
public class TenantUpgradeServiceImpl implements TenantUpgradeService {

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        String customer = (String) parameters.get("customer");
        String model = (String) parameters.get("model");
        Boolean all = (Boolean) parameters.get("all");

        switch (command) {
            default:
                return false;
        }

    }

}
