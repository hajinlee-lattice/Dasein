package com.latticeengines.upgrade.service;

public interface UpgradeService extends CommandExecutor {

    void switchToVersion(String version);
}
