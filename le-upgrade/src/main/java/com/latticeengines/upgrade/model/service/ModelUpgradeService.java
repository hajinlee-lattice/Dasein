package com.latticeengines.upgrade.model.service;

import java.util.Map;
import java.util.Set;

import com.latticeengines.upgrade.service.UpgradeService;

public interface ModelUpgradeService extends UpgradeService {

    Map<String, String> getUuidModelNameMap();

    Set<String> getLpUuidsBeforeUpgrade();

}
