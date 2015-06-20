package com.latticeengines.upgrade.model.service;

import java.util.Map;

public interface ModelUpgradeService {

    void upgrade() throws Exception;

    void execute(String command, Map<String, Object> parameters);
}
