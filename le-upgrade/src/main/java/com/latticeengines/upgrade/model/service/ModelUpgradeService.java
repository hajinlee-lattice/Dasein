package com.latticeengines.upgrade.model.service;

import com.latticeengines.upgrade.service.CommandExecutor;

public interface ModelUpgradeService extends CommandExecutor {

    void upgrade() throws Exception;

    void setVersion(String version);

}
