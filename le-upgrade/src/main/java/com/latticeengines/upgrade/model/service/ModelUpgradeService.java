package com.latticeengines.upgrade.model.service;

import java.util.Set;

import com.latticeengines.upgrade.service.UpgradeService;

public interface ModelUpgradeService extends UpgradeService {
    Set<String> getLpUuidsBeforeUpgrade();

    void exportModelsFromBardToHdfs(String customer);
}
