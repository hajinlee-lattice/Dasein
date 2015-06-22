package com.latticeengines.upgrade.service;

import java.util.Map;

public interface UpgradeService {

    void switchToVersion(String version);

    void execute(String command, Map<String, Object> parameters) throws Exception;
}
